/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "sstream"
#include "Validator.h"
#include <thread>
#include "Logger.h"

namespace QDB {

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
#ifdef  QDBTXRECOVERY
  txLog(*new TxLog(true, _rpcService ?_rpcService->getServerAddress() : "0.0.0.0")) {
#else
  txLog(*new TxLog(false, _rpcService ?_rpcService->getServerAddress() : "0.0.0.0")) {
#endif
    lastScheduledTxCTS = 0;

    // Fixme: may need to use TBB to pin the threads to specific cores LATER
    if (!isUnderTest) {
#ifdef  QDBTXRECOVERY
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
            abort(); //make sure the size of over-provisioned array has been corrected

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
    RAMCLOUD_LOG(NOTICE, "precommitReadErr");
    return false;
}

bool
Validator::insertConcludeQueue(TxEntry *txEntry) {
      concludeQueue.push(txEntry);
      RAMCLOUD_LOG(NOTICE, "insert concludeQueue  cts %lu %lu in %lu out %lu",
              (uint64_t)((txEntry)->getCTS() >> 64), (uint64_t)((txEntry)->getCTS() & (((__uint128_t)1<<64) -1)),
              concludeQueue.inCount.load(), concludeQueue.outCount.load());
      return true;
//    return concludeQueue.push(txEntry);
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
    TxEntry *prevTxEntry = NULL;
    uint64_t lastTick = 0;
    do {
        if ((txEntry = (TxEntry *)reorderQueue.try_pop(isUnderTest ? (__uint128_t)-1 : get128bClockValue()))) {
            if (txEntry->getCTS() == lastScheduledTxCTS) {
                RAMCLOUD_LOG(NOTICE, "duplicate %lu", (uint64_t)(txEntry->getCTS() >> 64));
                counters.duplicates++;
                continue; //ignore the duplicate
            }

            peerInfo.monitor(txEntry->getCTS(), this);

            if (txEntry->getCTS() < lastScheduledTxCTS) {
                RAMCLOUD_LOG(NOTICE, "late %lu last %lu",
                        (uint64_t)(txEntry->getCTS() >> 64),
                        (uint64_t)(lastScheduledTxCTS >> 64));

                assert(prevTxEntry != txEntry);

                //abort this CI, which has arrived later than a scheduled CI
                //aborting is fine because its SSN info has never been sent to its peers
                //although, in recovery case, SSN info could have been sent to its peers, the order of recovery should be preserved
                //set the states and let peerInfo handling thread to do the rest
                txEntry->setTxState(TxEntry::TX_OUTOFORDER);
                counters.lateScheduleErrors++;
                continue;
            }
            while (!distributedTxSet.add(txEntry));
            RAMCLOUD_LOG(NOTICE, "schedule %lu",(uint64_t)(txEntry->getCTS() >> 64));
            lastScheduledTxCTS = txEntry->getCTS();
            prevTxEntry = txEntry;
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
	    if (rpcService) {
	        rpcService->recordTxCommitDispatch(txEntry);
	    }
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

                if (conclude(txEntry)) {
                    logTx(LOG_DEBUG, txEntry); //for debugging only, not for recovery
                    localTxQueue.remove(it, txEntry);
                }
            }
            hasEvent = true;
            txEntry = localTxQueue.findNext(it);
        }

        // process due commit-intents on cross-shard transaction queue
        while ((txEntry = distributedTxSet.findReadyTx(activeTxSet))) {
            if (rpcService) {
                rpcService->recordTxCommitDispatch(txEntry);
            }

            //enable blocking incoming dependent transactions
            if (!activeTxSet.add(txEntry))
		abort();


            //enable sending SSN info to peer
            txEntry->setTxCIState(TxEntry::TX_CI_SCHEDULED);
            hasEvent = true;

            RAMCLOUD_LOG(NOTICE, "activate  cts %lu %lu cnt %lu",
                         (uint64_t)((txEntry)->getCTS() >> 64), (uint64_t)((txEntry)->getCTS() & (((__uint128_t)1<<64) -1)),
                         activeTxSet.getRemovedTxCount());
        }

        while (concludeQueue.try_pop(txEntry)) {
            conclude(txEntry);

            RAMCLOUD_LOG(NOTICE, "pop concludeQueue  cts %lu %lu in %lu  out %lu",
                    (uint64_t)((txEntry)->getCTS() >> 64), (uint64_t)((txEntry)->getCTS() & (((__uint128_t)1<<64) -1)),
                    concludeQueue.inCount.load(), concludeQueue.outCount.load());
            hasEvent = true;
        }
    } //end while(true)
}

bool
Validator::conclude(TxEntry *txEntry) {
    //record results and meta data
    if (txEntry->getTxState() == TxEntry::TX_COMMIT) {
        updateKVReadSetPStamp(*txEntry);
        updateKVWriteSet(*txEntry);
    }

    if (txEntry->getPeerSet().size() >= 1) {
        //for late cross-shard tx, it is never added to activeTxSet; just convert the state
        if (txEntry->getTxState() == TxEntry::TX_OUTOFORDER) {
            txEntry->setTxState(TxEntry::TX_ABORT);
        } else {
            activeTxSet.remove(txEntry);
        }

        RAMCLOUD_LOG(NOTICE, "conclude distTx %lu state %u", (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState());
    } else {
        RAMCLOUD_LOG(NOTICE, "conclude localTx %lu state %u", (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState());
    }

    sendTxCommitReply(txEntry);

    if (txEntry->getTxState() == TxEntry::TX_COMMIT)
        counters.commits++;
    else if (txEntry->getTxState() == TxEntry::TX_ABORT)
        counters.aborts++;
    else {
        counters.concludeErrors++;
        RAMCLOUD_LOG(NOTICE, "concludeErr %lu %u peerSet %lu", (uint64_t)(txEntry->getCTS() >> 64), txEntry->getTxState(), txEntry->getPeerSet().size());
        abort();
    }

    txEntry->setTxCIState(TxEntry::TX_CI_FINISHED);

    if (txEntry->getPeerSet().size() >= 1) {
        peerInfo.remove(txEntry->getCTS(), this);
    }

    //for ease of managing memory, do not free during unit test
    if (isUnderTest) {
        return true;
    }

    delete txEntry;
    return true;
}

void
Validator::peer() {
    PeerInfoIterator it;
    do {
        peerInfo.send(this);
    } while (isAlive && !isUnderTest);
}

bool
Validator::insertTxEntry(TxEntry *txEntry) {
    counters.commitIntents.fetch_add(1);

    if ((txEntry->getPStamp() >= txEntry->getSStamp())
            && (txEntry->getPStamp() >= (txEntry->getCTS() >> 64) &&  txEntry->getCTS() != 0)) {
        //Clearly the commit intent will be aborted -- the client should not even have
        //initiated, and we should not further burden the pipeline.
        counters.trivialAborts.fetch_add(1);
        txEntry->setTxState(TxEntry::TX_ABORT);
        txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
        return false; //skip queueing
    }

    if (txEntry->getPeerSet().size() == 0) {
        //single-shard tx
        RAMCLOUD_LOG(NOTICE, "insert localTx cts %lu txEntry %lu cnt %lu",
                (uint64_t)(txEntry->getCTS() >> 64), (uint64_t)txEntry,
                localTxQueue.addedTxCount.load());
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
        if (!localTxQueue.add(txEntry)) {
            counters.busyAborts.fetch_add(1);
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            return false; //fail to be queued
        }
    } else {
        //cross-shard tx
        RAMCLOUD_LOG(NOTICE, "insert distTx cts %lu txEntry %lu cnt %lu",
                (uint64_t)(txEntry->getCTS() >> 64), (uint64_t)txEntry,
                counters.queuedDistributedTxs.load());
        std::set<uint64_t>::iterator it;
        /*for (it = txEntry->getPeerSet().begin(); it != txEntry->getPeerSet().end(); it++) {
            RAMCLOUD_LOG(NOTICE, "peerId %lu", *it);
        }*/
        txEntry->setTxCIState(TxEntry::TX_CI_QUEUED); //set it before inserting to queue lest another thread might set CIState first

        while (!peerInfo.add(txEntry->getCTS(), txEntry, this));

        if (!reorderQueue.insert(txEntry->getCTS(), txEntry)) {
            counters.busyAborts.fetch_add(1);
            txEntry->setTxState(TxEntry::TX_ABORT);
            txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
            peerInfo.remove(txEntry->getCTS(), this);
            return false; //fail to be queued
        }

        counters.queuedDistributedTxs.fetch_add(1);
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
Validator::receiveSSNInfo(uint64_t peerId, __uint128_t cts,
        uint64_t pstamp, uint64_t sstamp, uint32_t peerTxState,
        uint64_t &myPStamp, uint64_t &mySStamp, uint32_t &myTxState) {
    RAMCLOUD_LOG(NOTICE, "receive cts %lu from %lu peerState %u ",
            (uint64_t)(cts >> 64), peerId, peerTxState);

    TxEntry *txEntry = NULL;
    bool isFound;
    if ((txEntry = peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, this, isFound)) == NULL) {
        if (txEntry != NULL) abort();
        if (txLog.getTxInfo(cts, myTxState, myPStamp, mySStamp)
                && myTxState != TxEntry::TX_PENDING) {
            //The commit intent is concluded
            counters.latePeers++;
            return NULL;
        }

        //Handle the fact that peer info is received before its tx commit intent is received,
        //hence not finding an existing peer entry,
        //by creating a peer entry without commit intent txEntry
        //and then updating the peer entry.
        /////peerInfo.addPartial(cts, peerId, peerTxState, pstamp, sstamp, this); //Fixme
        while (!peerInfo.add(cts, NULL, this));

        //Because the global mutex has been unlocked above, the CI might have been processed
        //by other threads before the following call is completed, so the peerEntry may or may not be
        //found, and the returning txEntry may or may not be found.
        txEntry = peerInfo.update(cts, peerId, peerTxState, pstamp, sstamp, this, isFound);

        counters.earlyPeers++;
    }
    counters.infoReceives++;
    if (txEntry && txEntry->getCTS() != cts) abort();
    return txEntry;
}

void
Validator::replySSNInfo(uint64_t peerId, __uint128_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
    assert(peerTxState != TxEntry::TX_CONFLICT);

    if (rpcService == NULL) //unit test may make rpcService NULL
        return;

    uint32_t myTxState = TxEntry::TX_PENDING;
    uint64_t myPStamp, mySStamp;
    TxEntry *txEntry = receiveSSNInfo(peerId, cts, pstamp, sstamp, peerTxState,
            myPStamp, mySStamp, myTxState);

    if (txEntry) {
        rpcService->sendDSSNInfo(cts, txEntry, true, peerId);
        counters.infoReplies++;
    } else if (myTxState != TxEntry::TX_PENDING) {
        //This is the case when a conclusion is found in txLog
        rpcService->sendDSSNInfo(cts, myTxState, myPStamp, mySStamp, peerId);
        RAMCLOUD_LOG(NOTICE, "infoLogReplies %lu %u", (uint64_t)(cts >> 64), myTxState);
        counters.infoLogReplies++;
    } else {
        //This is the case when the local CI has not been created
        //no reply at all
        RAMCLOUD_LOG(NOTICE, "cannot replySSNInfo %lu", (uint64_t)(cts >> 64));
    }
}

void
Validator::sendSSNInfo(TxEntry *txEntry, bool isSpecific, uint64_t targetPeerId) {
    if (rpcService) {
        if (!isSpecific)
            rpcService->sendDSSNInfo(txEntry->getCTS(), txEntry);
        else
            rpcService->sendDSSNInfo(txEntry->getCTS(), txEntry, true, targetPeerId);
        RAMCLOUD_LOG(NOTICE, "send: cts %lu target %lu", (uint64_t)(txEntry->getCTS() >> 64), targetPeerId);
        counters.infoSends.fetch_add(1);
    }
}

void
Validator::requestSSNInfo(TxEntry *txEntry, bool isSpecific, uint64_t targetPeerId) {
    if (rpcService) {
        rpcService->requestDSSNInfo(txEntry, isSpecific, targetPeerId);
        RAMCLOUD_LOG(NOTICE, "request: cts %lu target %lu", (uint64_t)(txEntry->getCTS() >> 64), targetPeerId);
        counters.infoRequests.fetch_add(1);
    }
}

void
Validator::sendTxCommitReply(TxEntry *txEntry) {
    if (rpcService) {
        rpcService->sendTxCommitReply(txEntry);
        RAMCLOUD_LOG(NOTICE, "commitReply: cts %lu", (uint64_t)(txEntry->getCTS() >> 64));
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
        counters.recovers.fetch_add(1);
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
    char val[3000];
    int c = 0;
    int s = sizeof(val);
    c += snprintf(val + c, s - c, "serverId:%lu, ", counters.serverId.load());
    c += snprintf(val + c, s - c, "initialWrites:%lu, ", counters.initialWrites.load());
    c += snprintf(val + c, s - c, "rejectedWrites:%lu, ", counters.rejectedWrites.load());
    c += snprintf(val + c, s - c, "precommitReads:%lu, ", counters.precommitReads.load());
    c += snprintf(val + c, s - c, "commitIntents:%lu, ", counters.commitIntents.load());
    c += snprintf(val + c, s - c, "recovers:%lu, ", counters.recovers.load());
    c += snprintf(val + c, s - c, "duplicates:%lu, ", counters.duplicates.load());
    c += snprintf(val + c, s - c, "trivialAborts:%lu, ", counters.trivialAborts.load());
    c += snprintf(val + c, s - c, "busyAborts:%lu, ", counters.busyAborts.load());
    c += snprintf(val + c, s - c, "ctsSets:%lu, ", counters.ctsSets.load());
    c += snprintf(val + c, s - c, "queuedLocalTxs:%lu, ", localTxQueue.addedTxCount.load());
    c += snprintf(val + c, s - c, "evaluatedLocalTxs:%lu, ", localTxQueue.removedTxCount.load());
    c += snprintf(val + c, s - c, "addPeers:%lu, ", counters.addPeers.load());
    c += snprintf(val + c, s - c, "earlyPeers:%lu, ", counters.earlyPeers.load());
    c += snprintf(val + c, s - c, "matchEarlyPeers:%lu, ", counters.matchEarlyPeers.load());
    c += snprintf(val + c, s - c, "latePeers:%lu, ", counters.latePeers.load());
    c += snprintf(val + c, s - c, "deletedPeers:%lu, ", counters.deletedPeers.load());
    c += snprintf(val + c, s - c, "queuedDistributedTxs:%lu, ", counters.queuedDistributedTxs.load());
    c += snprintf(val + c, s - c, "scheduledDistributedTxs:%lu, ", distributedTxSet.addedTxCount.load());
    c += snprintf(val + c, s - c, "evaluatedDistributedTxs:%lu, ", distributedTxSet.removedTxCount.load());
    c += snprintf(val + c, s - c, "concludeQueueIns:%lu, ", concludeQueue.inCount.load());
    c += snprintf(val + c, s - c, "concludeQueueOuts:%lu, ", concludeQueue.outCount.load());
    c += snprintf(val + c, s - c, "infoSends:%lu, ", counters.infoSends.load());
    c += snprintf(val + c, s - c, "infoReceives:%lu, ", counters.infoReceives.load());
    c += snprintf(val + c, s - c, "infoRequests:%lu, ", counters.infoRequests.load());
    c += snprintf(val + c, s - c, "infoReplies:%lu, ", counters.infoReplies.load());
    c += snprintf(val + c, s - c, "infoLogReplies:%lu, ", counters.infoLogReplies.load());
    c += snprintf(val + c, s - c, "precommitReadErrors:%lu, ", counters.precommitReadErrors.load());
    c += snprintf(val + c, s - c, "precommitWriteErrors:%lu, ", counters.precommitWriteErrors.load());
    c += snprintf(val + c, s - c, "preputErrors:%lu, ", counters.preputErrors.load());
    c += snprintf(val + c, s - c, "lateScheduleErrors:%lu, ", counters.lateScheduleErrors.load());
    c += snprintf(val + c, s - c, "readVersionErrors:%lu, ", counters.readVersionErrors.load());
    c += snprintf(val + c, s - c, "concludeErrors:%lu, ", counters.concludeErrors.load());
    c += snprintf(val + c, s - c, "alertAborts:%lu, ", counters.alertAborts.load());
    c += snprintf(val + c, s - c, "commits:%lu, ", counters.commits.load());
    c += snprintf(val + c, s - c, "aborts:%lu, ", counters.aborts.load());
    c += snprintf(val + c, s - c, "commitReads:%lu, ", counters.commitReads.load());
    c += snprintf(val + c, s - c, "commitWrites:%lu, ", counters.commitWrites.load());
    c += snprintf(val + c, s - c, "commitOverwrites:%lu, ", counters.commitOverwrites.load());
    c += snprintf(val + c, s - c, "commitDeletes:%lu, ", counters.commitDeletes.load());

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

