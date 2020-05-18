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
		  concludeQueue(*new ConcludeQueue())
		  {
	localTxCTSBase = clock.getLocalTime();

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
        	counters.commitWrites++;
        } else {
        	kvStore.putNew(writeSet[i], txEntry.getCTS(), txEntry.getSStamp());
        	writeSet[i] = 0; //prevent txEntry destructor from freeing the KVLayout pointer
        	counters.commitOverwrites++;
        }
    }
    return true;
}

#if 0 //unused for now
bool
Validator::write(KLayout& k, uint64_t &vPrevPStamp) {
    DSSNMeta meta;
    KVLayout *kv = kvStore.fetch(k);
    if (kv) {
        meta = kv->meta();
        vPrevPStamp = meta.pStampPrev;
        return true;
    }
    return false;
}
#endif

bool
Validator::read(KLayout& k, KVLayout *&kv) {
    //FIXME: This read can happen concurrently while conclude() is
    //modifying the KVLayout instance.
    kv = kvStore.fetch(k);
    counters.reads++;
    return (kv!=NULL && !kv->isTombstone());
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
	TxEntry *txEntry;
    do {
    	if ((txEntry = (TxEntry *)reorderQueue.try_pop(clock.getLocalTime()))) {
    		if (txEntry->getCTS() < localTxCTSBase) {
    		    //Fixme:
    		    counters.lateScheduleErrors++;
    			continue; //ignore this CI that is past a processed CI
    		}
    		while (!distributedTxSet.add(txEntry));
    		localTxCTSBase = txEntry->getCTS();
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

        		// As local transactions can be validated in any order, we can set the CTS.
        		// Use the last largest cross-shard CTS as base. That has very little impact on cross-shard txs.
        		txEntry->setCTS(++localTxCTSBase);

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
        	peerInfo.add(txEntry);

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

	txEntry.setTxCIState(TxEntry::TX_CI_FINISHED); //redundant if txEntry is deleted right after

	//for ease of managing memory, do not free during unit test
	if (!isUnderTest)
		delete &txEntry;

	return true;
}

void
Validator::peer() {
	PeerInfoIterator it;
	TxEntry *txEntry;
	do {
		if (rpcService) {
			txEntry = peerInfo.getFirst(it);
			while (txEntry) {
				if (txEntry->getTxCIState() == TxEntry::TX_CI_SCHEDULED
						&& txEntry->getTxState() != TxEntry::TX_ALERT) { //Fixme: not to set upon ALERT???
					//log CI before sending
					//Fixme: txLog.add(txEntry);
					rpcService->sendDSSNInfo(txEntry);
					txEntry->setTxCIState(TxEntry::TX_CI_LISTENING);
				}
				txEntry = peerInfo.getNext(it);
			}
		}
		peerInfo.sweep();
		this_thread::yield();
	} while (!isUnderTest);
}

bool
Validator::insertTxEntry(TxEntry *txEntry) {
    counters.commitIntents++;
	if (txEntry->getPeerSet().size() == 0) {
		//single-shard tx
		if (localTxQueue.add(txEntry)) {
			txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
			counters.queuedLocalTxs++;
			return true;
		}
	} else {
		//cross-shard tx
		if (distributedTxSet.add(txEntry)) {
			txEntry->setTxCIState(TxEntry::TX_CI_QUEUED);
			counters.queuedDistributedTxs++;
		}
	}
	return false;
}

void
Validator::receiveSSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
	TxEntry *txEntry;
	//Fixme: implement/use the TxState state machine --- here and/or peering thread???
	//Fixme: handle peer SSN info being received before txEntry creation!!!
	if (peerInfo.update(cts, peerId, pstamp, sstamp, txEntry)) {
		//Fixme: should following section be part of the above update() to be thread safe???
		if (txEntry->isExclusionViolated()) {
			txEntry->setTxState(TxEntry::TX_ABORT);
			if (peerTxState == TxEntry::TX_COMMIT) {
				txEntry->setTxState(TxEntry::TX_CONFLICT);
				assert(0); //Fixme: recover or debug
			}
		} else if (txEntry->isAllPeerSeen() && !txEntry->isExclusionViolated()) {
			txEntry->setTxState(TxEntry::TX_COMMIT);
			if (peerTxState == TxEntry::TX_ABORT) {
				txEntry->setTxState(TxEntry::TX_CONFLICT);
				assert(0); //Fixme: recover or debug
			}
		} else
			return; //inconclusive yet

		//Our validator restart design assumes logging going asynchronously
		//with saving write set to KV store, but
		//Logging must precede sending tx CI reply
		//Note that our overall scheme does not need to log local transactions at all
		//By now the tuples should have successfully been preput into the KV store, so
		//logging the CI conclusion is considered sealing a tx commit.
		if (txEntry->getTxCIState() < TxEntry::TX_CI_CONCLUDED
				&& true /* Fixme: txLog.add(txEntry) */) {
			txEntry->setTxCIState(TxEntry::TX_CI_CONCLUDED);
			assert(insertConcludeQueue(txEntry));
			rpcService->sendTxCommitReply(txEntry);
			if (txEntry->getTxState() == TxEntry::TX_COMMIT)
			    counters.commits++;
			else
			    counters.aborts++;
		}
	}
}

void
Validator::replySSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState) {
	TxEntry *txEntry;
	if (peerTxState == TxEntry::TX_CONFLICT)
		assert(0); //Fixme: do recovery or debug
	peerInfo.update(cts, peerId, pstamp, sstamp, txEntry);
	if (rpcService) //unit test may make it NULL
		rpcService->sendDSSNInfo(txEntry, true, peerId);
}

} // end Validator class

