/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "sstream"
#include "Validator.h"
#include <thread>

namespace DSSN {

const uint64_t maxTimeStamp = std::numeric_limits<uint64_t>::max();
const uint64_t minTimeStamp = 0;

Validator::Validator(HashmapKVStore &_kvStore, DSSNService *_rpcService, bool _isTesting)
: kvStore(_kvStore),
  rpcService(_rpcService),
  isUnderTest(_isTesting),
  localTxQueue(*new WaitList(1000001)),
  reorderQueue(*new SkipList<__uint128_t>()),
  distributedTxSet(*new DistributedTxSet()),
  activeTxSet(*new ActiveTxSet()),
  peerInfo(*new PeerInfo()),
  concludeQueue(*new ConcludeQueue()),
#ifdef  DSSNTXRECOVERY
  txLog(*new TxLog(true, _rpcService ?_rpcService->getServerAddress() : "0.0.0.0")) {
#else
  txLog(*new TxLog(false, _rpcService ?_rpcService->getServerAddress() : "0.0.0.0")) {
#endif
    lastScheduledTxCTS = 0;

    // Fixme: may need to use TBB to pin the threads to specific cores LATER
    if (!isUnderTest) {
#ifdef  DSSNTXRECOVERY
        recover();
#endif
        serializeThread = std::thread(&Validator::serialize, this);
        peeringThread = std::thread(&Validator::peer, this);
        schedulingThread = std::thread(&Validator::scheduleDistributedTxs, this);

    }
}

Validator::~Validator() {
    if (!isUnderTest) {
        isAlive = false;
        if (serializeThread.joinable())
            serializeThread.join();
        if (peeringThread.joinable())
            peeringThread.join();
        if (schedulingThread.joinable())
            schedulingThread.join();
        logCounters();
    }
    delete &localTxQueue;
    delete &reorderQueue;
    delete &distributedTxSet;
    delete &activeTxSet;
    delete &peerInfo;
    delete &concludeQueue;
}

bool
Validator::testRun() {
    if (!isUnderTest)
        return false;
    scheduleDistributedTxs();
    serialize();
    peer();
    return true;
}

bool
Validator::updateTxPStampSStamp(TxEntry &txEntry) {
    /*
     * Find out my largest predecessor (pstamp) and smallest successor (sstamp).
     * For reads, see if another has over-written the tuples by checking successor LSN.
     * For writes, see if another has read the tuples by checking access LSN.
     *
     * We use single-version in-memory KV store. Any stored tuple is the latest
     * committed version. Therefore, we are implementing SSN over RC (Read-Committed).
     * Moreover, the validator does not store the uncommitted write set; the tx client
     * is to pass the write set (and read set) through the commit-intent.
     */

    txEntry.setSStamp(std::min(txEntry.getSStamp(), uint64_t(txEntry.getCTS() >> 64)));

    //update sstamp of transaction
    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < txEntry.getReadSetSize(); i++) {
        if (readSet[i] == NULL)
            continue; //the array may be over-provisioned due to read-modify-write tx handling

        KVLayout *kv = kvStore.fetch(readSet[i]->k);
        if (kv) {
            //A safety check to ensure that the version in the kv store
            //is the same one when the read has occurred.
            //A missing version, possibly due to failure recovery or
            //our design choice of not keeping long version chains,
            //would cause an abort.
            if (kv->meta().cStamp != readSet[i]->meta().cStamp) {
                txEntry.setSStamp(0); //deliberately cause an exclusion window violation
                counters.readVersionErrors++;
                return false;
            }

            txEntry.setSStamp(std::min(txEntry.getSStamp(), kv->meta().sStamp));
            if (txEntry.isExclusionViolated()) {
                return false;
            }
        }
        txEntry.insertReadSetInStore(kv, i);
    }

    //update pstamp of transaction
    auto  &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < txEntry.getWriteSetSize(); i++) {
        KVLayout *kv = kvStore.fetch(writeSet[i]->k);
        if (kv) {
            txEntry.setPStamp(std::max(txEntry.getPStamp(), kv->meta().pStamp));
            if (txEntry.isExclusionViolated()) {
                return false;
            }
        }
        txEntry.insertWriteSetInStore(kv, i);
    }

    return true;
}

bool
Validator::updateKVReadSetPStamp(TxEntry &txEntry) {
    auto &readSet = txEntry.getReadSetInStore();
    for (uint32_t i = 0; i < txEntry.getReadSetSize(); i++) {
        if (readSet[i]) {
            readSet[i]->meta().pStamp = std::max(uint64_t(txEntry.getCTS() >> 64), readSet[i]->meta().pStamp);
            counters.commitReads++;
        } else {
            //may be over-provisioned to cover read-modify-write case; do nothing

            //The condition that (txEntry.getReadSet()[i] != NULL) should not be hit:
            //the tuple is removed (by another concurrent tx) and garbage-collected
            //before the current tx is committed.
            //assert(txEntry.getReadSet()[i] == NULL); //commented out for performance reason
        }
    }
    return true;
}

bool
Validator::updateKVWriteSet(TxEntry &txEntry) {
    auto &writeSet = txEntry.getWriteSet();
    auto &writeSetInStore = txEntry.getWriteSetInStore();
    for (uint32_t i = 0; i < txEntry.getWriteSetSize(); i++) {
        if (writeSetInStore[i]) {
            kvStore.put(writeSetInStore[i], txEntry.getCTS(), txEntry.getSStamp(),
                    writeSet[i]->v.valuePtr, writeSet[i]->v.valueLength);
            if (writeSetInStore[i]->v.isTombstone)
                counters.commitDeletes++;
            else
                counters.commitOverwrites++;
            //No need to nullify writeSet[i] so that txEntry destructor would free KVLayout memory
        } else {
            kvStore.putNew(writeSet[i], txEntry.getCTS(), txEntry.getSStamp());
            counters.commitWrites++;
            writeSet[i] = 0; //prevent txEntry destructor from freeing the KVLayout memory
        }
    }
    return true;
}

#if 0 //precommit write does not reach validator for now
bool
Validator::write(KVLayout& kv) {
    //treat it as a single-write transaction
    KVLayout *kvExisting = kvStore.fetch(kv.k);

    if (kvExisting == NULL) {
        KVLayout *nkv = kvStore.preput(kv);
        kvStore.putNew(nkv, 0, 0xffffffffffffffff);
    } else {
        void* pval = new char[kv.v.valueLength];
        std::memcpy(pval, kv.v.valuePtr, kv.v.valueLength);
        kvStore.put(kvExisting, 0, 0xffffffffffffffff, (uint8_t*)pval, kv.v.valueLength);
        assert(0);
    }
}
#endif

bool
Validator::initialWrite(KVLayout &kv) {
    //the function is supposed to be used for testing purpose for initializing some tuples
    ///Disabled initial write if not testing, for fear that:
    ///there could still be a concurrent tx commit that races against this backdoor write.

    if (!isUnderTest) {
        counters.rejectedWrites++;
        return false;
    }

    KVLayout *existing = kvStore.fetch(kv.k);
    if (existing != NULL) {
        assert(0);
        return false;
    }
    KVLayout *nkv = kvStore.preput(kv);
    if (nkv != NULL && kvStore.putNew(nkv, 0, 0xffffffffffffffff)) {
        counters.initialWrites++;
        return true;
    }
    counters.rejectedWrites++;
    return false;
}

bool
Validator::read(KLayout& k, KVLayout *&kv) {
    //FIXME: This read can happen concurrently while conclude() is
    //modifying the KVLayout instance.
    kv = kvStore.fetch(k);
    if (kv != NULL && !kv->isTombstone()) {
        counters.precommitReads++;
        return true;
    }
    counters.precommitReadErrors++;
    return false;
}

bool
Validator::insertConcludeQueue(TxEntry *txEntry) {
    return concludeQueue.push(txEntry);
}


bool
Validator::validateLocalTx(TxEntry& txEntry) {
    //calculate local pstamp and sstamp
    updateTxPStampSStamp(txEntry);

    if (txEntry.isExclusionViolated()) {
        txEntry.setTxState(TxEntry::TX_ABORT);
    } else {
        txEntry.setTxState(TxEntry::TX_COMMIT);
    }
    txEntry.setTxCIState(TxEntry::TX_CI_CONCLUDED);

    return true;
};

void
Validator::scheduleDistributedTxs() {
    //Move commit intents from reorder queue when they are due in view of local clock,
    //expressed in the cluster time unit (due to current sequencer implementation).
    //During testing, ignore the timing constraint imposed by the local clock.
    TxEntry *txEntry;
    uint64_t lastTick = 0;
    do {
        if ((txEntry = (TxEntry *)reorderQueue.try_pop(isUnderTest ? (__uint128_t)-1 : get128bClockValue()))) {
            if (txEntry->getCTS() <= lastScheduledTxCTS) {
                assert(txEntry->getCTS() != lastScheduledTxCTS); //no duplicate CTS from sequencer

                //abort this CI, which has arrived later than a scheduled CI
                //aborting is fine because its SSN info has never been sent to its peers
                //although, in recovery case, SSN info could have been sent to its peers, the order of recovery should be preserved
                //set the states and let peerInfo handling thread to do the rest
                txEntry->setTxState(TxEntry::TX_OUTOFORDER);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                counters.lateScheduleErrors++;
                continue;
            }
            while (!distributedTxSet.add(txEntry));
            lastScheduledTxCTS = txEntry->getCTS();
        }

        //log counters every 10s
        if (!isUnderTest && logLevel >= LOG_INFO) {
            uint64_t nsTime = getClockValue();
            uint64_t currentTick = nsTime / 10000000000;
            if (lastTick < currentTick) {
                logCounters();
                lastTick = currentTick;
            }
        }
    } while (isAlive && !isUnderTest);
}

void
Validator::serialize() {
    /*
     * This loop handles the DSSN serialization window critical section
     */
    bool hasEvent = true;
    while (isAlive && (!isUnderTest || hasEvent)) {
        hasEvent = false;

        // process all commit-intents on local transaction queue
        TxEntry* txEntry;
        uint64_t it;
        txEntry = localTxQueue.findFirst(it);
        while (txEntry) {
            assert(txEntry->getPeerSet().size() == 0);
            if (!activeTxSet.blocks(txEntry)) {
                /* There is no need to update activeTXs because this tx is validated
                 * and concluded shortly. If the conclude() does through a queue and another
                 * task, then we should add tx to active tx set here.
                 */

                if (txEntry->getCTS() == 0) {
                    //This feature may allow tx client to do without a clock
                    txEntry->setCTS(get128bClockValue());
                    counters.ctsSets++;
                }

                validateLocalTx(*txEntry);

                if (conclude(*txEntry)) {
                    logTx(LOG_DEBUG, txEntry); //for debugging only, not for recovery
                    localTxQueue.remove(it, txEntry);
                }
            }
            hasEvent = true;
            txEntry = localTxQueue.findNext(it);
        }

        // process due commit-intents on cross-shard transaction queue
        while ((txEntry = distributedTxSet.findReadyTx(activeTxSet))) {
            //enable blocking incoming dependent transactions
            assert(activeTxSet.add(txEntry));

            //enable sending SSN info to peer
            txEntry->setTxCIState(TxEntry::TX_CI_SCHEDULED);

            hasEvent = true;
        }

        while (concludeQueue.try_pop(txEntry)) {
            conclude(*txEntry);
            hasEvent = true;
        }
    } //end while(true)
}

bool
Validator::conclude(TxEntry& txEntry) {
    //record results and meta data
    if (txEntry.getTxState() == TxEntry::TX_COMMIT) {
        updateKVReadSetPStamp(txEntry);
        updateKVWriteSet(txEntry);
    }

    if (txEntry.getPeerSet().size() >= 1) {
        //for late cross-shard tx, it is never added to activeTxSet; just convert the state
        if (txEntry.getTxState() == TxEntry::TX_OUTOFORDER)
            txEntry.setTxState(TxEntry::TX_ABORT);
        else
            activeTxSet.remove(&txEntry);
    }

    sendTxCommitReply(&txEntry);

    if (txEntry.getTxState() == TxEntry::TX_COMMIT)
        counters.commits++;
    else if (txEntry.getTxState() == TxEntry::TX_ABORT)
        counters.aborts++;
    else {
        counters.concludeErrors++;
        assert(0);
    }

    txEntry.setTxCIState(TxEntry::TX_CI_FINISHED);

    //for ease of managing memory, do not free during unit test
    if (!isUnderTest)
        delete &txEntry;

    return true;
}

void
Validator::peer() {
    PeerInfoIterator it;
    do {
        peerInfo.send(this);
        peerInfo.sweep(this);
        this_thread::yield();
    } while (isAlive && !isUnderTest);
}

bool
Validator::insertTxEntry(TxEntry *txEntry) {
    counters.commitIntents++;

    if ((txEntry->getPStamp() >= txEntry->getSStamp())
            && (txEntry->getPStamp() >= (txEntry->getCTS() >> 64) &&  txEntry->getCTS() != 0)) {
        //Clearly the commit intent will be aborted -- the client should not even have
        //initiated, and we should not further burden the pipeline.
        counters.trivialAborts++;
        txEntry->setTxState(TxEntry::TX_ABORT);
        txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        return false; //skip queueing
    }

    if (txEntry->getPeerSet().size() == 0) {
        //single-shard tx
        if (!localTxQueue.add(txEntry)) {
            counters.busyAborts++;
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            return false; //fail to be queued
        }
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
    } else {
        //cross-shard tx
        if (!reorderQueue.insert(txEntry->getCTS(), txEntry)) {
            counters.busyAborts++;
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            return false; //fail to be queued
        }
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);

        assert(peerInfo.add(txEntry->getCTS(), txEntry, this));

        counters.queuedDistributedTxs++;
    }
    return true;
}

uint64_t
Validator::getClockValue() {
    //time in ns unit.
    return clock.getLocalTime();
}

__uint128_t
Validator::get128bClockValue() {
    return (__uint128_t)clock.getLocalTime() << 64;
}

TxEntry*
Validator::receiveSSNInfo(uint64_t peerId, __uint128_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
    TxEntry *txEntry = NULL;
    if (!peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, txEntry, this)) {
        //Handle the fact that peer info is received before its tx commit intent is received,
        //hence not finding an existing peer entry,
        //by creating a peer entry without commit intent txEntry
        //and then updating the peer entry.
        peerInfo.add(cts, NULL, this); //create a peer entry without commit intent txEntry
        assert(peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, txEntry, this));
        counters.earlyPeers++;
    }
    counters.infoReceives++;
    return txEntry;
}

void
Validator::replySSNInfo(uint64_t peerId, __uint128_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
    assert(peerTxState != TxEntry::TX_CONFLICT);

    if (rpcService == NULL) //unit test may make rpcService NULL
        return;

    TxEntry *txEntry = receiveSSNInfo(peerId, cts, pstamp, sstamp, peerTxState);
    uint32_t txState;
    uint64_t pStamp, sStamp;
    if (txEntry) {
        rpcService->sendDSSNInfo(cts, txEntry, true, peerId);
        counters.infoReplies++;
    } else if (txLog.getTxInfo(cts, txState, pStamp, sStamp)) {
        rpcService->sendDSSNInfo(cts, txState, pStamp, sStamp, peerId);
        counters.infoReplies++;
    } else {
        //This is the case when the local CI has not been created
        //no reply at all
    }
}

void
Validator::sendSSNInfo(TxEntry *txEntry, bool isSpecific, uint64_t targetPeerId) {
    if (rpcService) {
        if (!isSpecific)
            rpcService->sendDSSNInfo(txEntry->getCTS(), txEntry);
        else
            rpcService->sendDSSNInfo(txEntry->getCTS(), txEntry, true, targetPeerId);
        counters.infoSends++;
    }
}

void
Validator::requestSSNInfo(TxEntry *txEntry, bool isSpecific, uint64_t targetPeerId) {
    if (rpcService) {
        rpcService->requestDSSNInfo(txEntry, isSpecific, targetPeerId);
        counters.infoRequests++;
    }
}

void
Validator::sendTxCommitReply(TxEntry *txEntry) {
    if (rpcService) {
        rpcService->sendTxCommitReply(txEntry);
    }
}

bool
Validator::recover() {
    uint64_t it = 0;
    DSSNMeta meta;
    TxEntry *txEntry = new TxEntry(0, 0);
    while (txLog.getNextPendingTx(it, it, meta, txEntry->getPeerSet(),
            txEntry->getWriteSet())) {
        txEntry->setTxState(TxEntry::TX_ALERT); //indicate a recovered entry
        insertTxEntry(txEntry);
        counters.recovers++;
        txEntry = new TxEntry(0, 0);
    }
    delete txEntry;

    return true;
}

bool
Validator::logCounters() {
    if (logLevel < LOG_INFO)
        return false;
    char key[] = {1};
    char val[2048];
    int c = 0;
    int s = sizeof(val);
    c += snprintf(val + c, s - c, "serverId:%lu, ", counters.serverId);
    c += snprintf(val + c, s - c, "initialWrites:%lu, ", counters.initialWrites);
    c += snprintf(val + c, s - c, "rejectedWrites:%lu, ", counters.rejectedWrites);
    c += snprintf(val + c, s - c, "precommitReads:%lu, ", counters.precommitReads);
    c += snprintf(val + c, s - c, "commitIntents:%lu, ", counters.commitIntents);
    c += snprintf(val + c, s - c, "recovers:%lu, ", counters.recovers);
    c += snprintf(val + c, s - c, "trivialAborts:%lu, ", counters.trivialAborts);
    c += snprintf(val + c, s - c, "busyAborts:%lu, ", counters.busyAborts);
    c += snprintf(val + c, s - c, "ctsSets:%lu, ", counters.ctsSets);
    c += snprintf(val + c, s - c, "queuedLocalTxs:%lu, ", localTxQueue.addedTxCount.load());
    c += snprintf(val + c, s - c, "evaluatedLocalTxs:%lu, ", localTxQueue.removedTxCount);
    c += snprintf(val + c, s - c, "addPeers:%lu, ", counters.addPeers);
    c += snprintf(val + c, s - c, "earlyPeers:%lu, ", counters.earlyPeers);
    c += snprintf(val + c, s - c, "matchEarlyPeers:%lu, ", counters.matchEarlyPeers);
    c += snprintf(val + c, s - c, "deletedPeers:%lu, ", counters.deletedPeers);
    c += snprintf(val + c, s - c, "queuedDistributedTxs:%lu, ", counters.queuedDistributedTxs);
    c += snprintf(val + c, s - c, "scheduledDistributedTxs:%lu, ", distributedTxSet.addedTxCount);
    c += snprintf(val + c, s - c, "evaluatedDistributedTxs:%lu, ", distributedTxSet.removedTxCount);
    c += snprintf(val + c, s - c, "infoSends:%lu, ", counters.infoSends);
    c += snprintf(val + c, s - c, "infoReceives:%lu, ", counters.infoReceives);
    c += snprintf(val + c, s - c, "infoRequests:%lu, ", counters.infoRequests);
    c += snprintf(val + c, s - c, "infoReplies:%lu, ", counters.infoReplies);
    c += snprintf(val + c, s - c, "precommitReadErrors:%lu, ", counters.precommitReadErrors);
    c += snprintf(val + c, s - c, "precommitWriteErrors:%lu, ", counters.precommitWriteErrors);
    c += snprintf(val + c, s - c, "preputErrors:%lu, ", counters.preputErrors);
    c += snprintf(val + c, s - c, "lateScheduleErrors:%lu, ", counters.lateScheduleErrors);
    c += snprintf(val + c, s - c, "readVersionErrors:%lu, ", counters.readVersionErrors);
    c += snprintf(val + c, s - c, "concludeErrors:%lu, ", counters.concludeErrors);
    c += snprintf(val + c, s - c, "alertAborts:%lu, ", counters.alertAborts);
    c += snprintf(val + c, s - c, "commits:%lu, ", counters.commits);
    c += snprintf(val + c, s - c, "aborts:%lu, ", counters.aborts);
    c += snprintf(val + c, s - c, "commitReads:%lu, ", counters.commitReads);
    c += snprintf(val + c, s - c, "commitWrites:%lu, ", counters.commitWrites);
    c += snprintf(val + c, s - c, "commitOverwrites:%lu, ", counters.commitOverwrites);
    c += snprintf(val + c, s - c, "commitDeletes:%lu, ", counters.commitDeletes);

    assert(s >= c);
    assert(strlen(val) < sizeof(val));
    return txLog.fabricate(get128bClockValue(), (uint8_t *)key, (uint32_t)sizeof(key),
            (uint8_t *)val, sizeof(val) /*(uint32_t)strlen(val) + 1*/);
}

bool
Validator::logTx(uint32_t currentLevel, TxEntry *txEntry) {
    if (logLevel < currentLevel)
        return false;
    return txLog.add(txEntry);
}

bool
Validator::logMessage(uint32_t currentLevel, const char* fmt, ...) {
    if (logLevel < currentLevel)
        return false;
    char key[] = {2};
    char val[2048];
    va_list args;
    va_start(args, fmt);
    vsnprintf(val, sizeof(val), fmt, args);
    va_end(args);
    return txLog.fabricate(get128bClockValue(), (uint8_t *)key, (uint32_t)sizeof(key),
            (uint8_t *)val, (uint32_t)strlen(val) + 1);
}

} // end Validator class

