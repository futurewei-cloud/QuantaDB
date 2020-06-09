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
  reorderQueue(*new SkipList()),
  distributedTxSet(*new DistributedTxSet()),
  activeTxSet(*new ActiveTxSet()),
  peerInfo(*new PeerInfo()),
  concludeQueue(*new ConcludeQueue()) {
    // Fixme: may need to use TBB to pin the threads to specific cores LATER
    if (!isUnderTest) {
        serializeThread = std::thread(&Validator::serialize, this);
        peeringThread = std::thread(&Validator::peer, this);
        schedulingThread = std::thread(&Validator::scheduleDistributedTxs, this);
    }
}

Validator::~Validator() {
    if (!isUnderTest) {
        if (serializeThread.joinable())
            serializeThread.detach();
        if (peeringThread.joinable())
            peeringThread.detach();
        if (schedulingThread.joinable())
            schedulingThread.detach();
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

    txEntry.setSStamp(std::min(txEntry.getSStamp(), txEntry.getCTS()));

    //update sstamp of transaction
    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < txEntry.getReadSetSize(); i++) {
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
            readSet[i]->meta().pStamp = std::max(txEntry.getCTS(), readSet[i]->meta().pStamp);
            counters.commitReads++;
        } else {
            counters.commitMetaErrors++;
            //In general this condition should not be hit:
            //the tuple is removed (by another concurrent tx) and garbage-collected
            //before the current tx is committed. Let's do nothing to it.
            //Unit test may trigger this condition.
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
            //No need to nullify writeSet[i] so that txEntry desutrctor would free KVLayout memory
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
    if (txEntry->getTxState() == TxEntry::TX_COMMIT)
        counters.commits++;
    else if (txEntry->getTxState() == TxEntry::TX_ABORT)
        counters.aborts++;
    else
        counters.concludeErrors++;
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
    do {
        if ((txEntry = (TxEntry *)reorderQueue.try_pop(isUnderTest ? (uint64_t)-1 : clock.getClusterTime(0)))) {
            if (txEntry->getCTS() <= lastScheduledTxCTS) {
                assert(txEntry->getCTS() != lastScheduledTxCTS); //no duplicate CTS from sequencer

                //abort this CI, which has arrived later than a scheduled CI
                //aborting is fine because its SSN info has never been sent to its peers
                txEntry->setTxState(TxEntry::TX_ABORT);
                txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
                sendTxCommitReply(txEntry);
                counters.lateScheduleErrors++;
                continue;
            }
            while (!distributedTxSet.add(txEntry));
            lastScheduledTxCTS = txEntry->getCTS();
        }
    } while (!isUnderTest);
}

void
Validator::serialize() {
    /*
     * This loop handles the DSSN serialization window critical section
     */
    bool hasEvent = true;
    while (!isUnderTest || hasEvent) {
        hasEvent = false;

        // process all commit-intents on local transaction queue
        TxEntry* txEntry;
        uint64_t it;
        txEntry = localTxQueue.findFirst(it);
        while (txEntry) {
            if (!activeTxSet.blocks(txEntry)) {
                /* There is no need to update activeTXs because this tx is validated
                 * and concluded shortly. If the conclude() does through a queue and another
                 * task, then we should add tx to active tx set here.
                 */

                validateLocalTx(*txEntry);

                if (conclude(*txEntry)) {
                    localTxQueue.remove(it);
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
        activeTxSet.remove(&txEntry);
    } else {
        rpcService->sendTxCommitReply(&txEntry);
        if (txEntry.getTxState() == TxEntry::TX_COMMIT)
            counters.commits++;
        else
            counters.aborts++;
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
        peerInfo.sweep();
        this_thread::yield();
    } while (!isUnderTest);
}

bool
Validator::insertTxEntry(TxEntry *txEntry) {
    counters.commitIntents++;
    if (txEntry->getPeerSet().size() == 0) {
        //single-shard tx
        if (!localTxQueue.add(txEntry))
            return false;
        if (txEntry->getCTS() == 0) {
            //This feature may allow tx client to do without a clock
            txEntry->setCTS(clock.getClusterTime());
            counters.ctsSets++;
        }
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
        counters.queuedLocalTxs++;
    } else {
        //cross-shard tx
        if (!reorderQueue.insert(txEntry->getCTS(), txEntry))
            return false;
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
        counters.queuedDistributedTxs++;

        peerInfo.add(txEntry->getCTS(), txEntry, this);
    }

    return true;
}

uint64_t
Validator::getClockValue() {
    //due to current clock service implementation, need conversion into ns unit.
    return clock.getClusterTime(0);
}

uint64_t
Validator::convertStampToClockValue(uint64_t timestamp) {
    //due to current clock service implementation, need conversion into ns unit.
    return clock.Cluster2Local(timestamp);
}

TxEntry*
Validator::receiveSSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
    TxEntry *txEntry = NULL;
    if (!peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, txEntry, this)) {
        //Handle the fact that peer info is received before its tx commit intent is received,
        //hence not finding an existing peer entry,
        //by creating a peer entry without commit intent txEntry
        //and then updating the peer entry.
        peerInfo.add(cts, NULL, this); //create a peer entry without commit intent txEntry
        peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, txEntry, this);
        counters.earlyPeers++;
    }
    return txEntry;
}

void
Validator::replySSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {

    if (peerTxState == TxEntry::TX_CONFLICT)
        assert(0); //Fixme: do recovery or debug
    if (rpcService == NULL) //unit test may make rpcService NULL
        return;

    //Fixme: if tx is already conlcuded and logged, provide the logged state
    uint8_t txState = TxEntry::TX_ALERT;
    TxEntry *txEntry = receiveSSNInfo(peerId, cts, pstamp, sstamp, peerTxState);
    if (txEntry) {
        txState = txEntry->getTxState();
    }

    rpcService->sendDSSNInfo(cts, txState, txEntry, true, peerId);
}

void
Validator::sendSSNInfo(TxEntry *txEntry) {
    if (rpcService)
        rpcService->sendDSSNInfo(txEntry->getCTS(), txEntry->getTxState(), txEntry);
}

void
Validator::requestSSNInfo(TxEntry *txEntry, uint64_t targetPeerId) {
    if (rpcService)
        rpcService->requestDSSNInfo(txEntry, true, targetPeerId);
}

void
Validator::sendTxCommitReply(TxEntry *txEntry) {
    if (rpcService)
        rpcService->sendTxCommitReply(txEntry);
}

} // end Validator class

