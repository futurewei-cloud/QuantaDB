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

void
Validator::start() {
    // Henry: may need to use TBB to pin the threads to specific cores LATER
    std::thread( [=] { serialize(); });
    std::thread( [=] { validateDistributedTxs(0); });
    std::thread( [=] { sweep(); });
    std::thread( [=] { scheduleDistributedTxs(); });
    localTxQueue.start();
}

bool
Validator::updateTxEtaPi(TxEntry &txEntry) {
    /*
     * Find out my largest predecessor (eta) and smallest successor (pi).
     * For reads, see if another has over-written the tuples by checking successor LSN.
     * For writes, see if another has read the tuples by checking access LSN.
     *
     * We use single-version in-memory KV store. Any stored tuple is the latest
     * committed version. Therefore, we are implementing SSN over RC (Read-Committed).
     * Moreover, the validator does not store the uncommitted write set; the tx client
     * is to pass the write set (and read set) through the commit-intent.
     */

    txEntry.setPi(std::min(txEntry.getPi(), txEntry.getCTS()));

    //update pi of transaction
    auto &readSet = txEntry.getReadSet();
    for (uint32_t i = 0; i < txEntry.getReadSetSize(); i++) {
    	KVLayout *kv = kvStore.fetch(readSet[i]->k);
    	if (kv) {
    		txEntry.setPi(std::min(txEntry.getPi(), kv->getMeta().sStamp));
    		if (txEntry.isExclusionViolated()) {
    			return false;
    		}
        }
    	txEntry.insertReadSetInStore(kv, i);
    }

    //update eta of transaction
    auto  &writeSet = txEntry.getWriteSet();
    for (uint32_t i = 0; i < txEntry.getWriteSetSize(); i++) {
    	KVLayout *kv = kvStore.fetch(writeSet[i]->k);
    	if (kv) {
    		txEntry.setEta(std::max(txEntry.getEta(), kv->getMeta().pStamp));
    		if (txEntry.isExclusionViolated()) {
    			return false;
    		}
    	}
    	txEntry.insertWriteSetInStore(kv, i);
    }

    return true;
}

bool
Validator::updateKVReadSetEta(TxEntry &txEntry) {
	auto &readSet = txEntry.getReadSetInStore();
	for (uint32_t i = 0; i < txEntry.getReadSetSize(); i++) {
		if (readSet[i]) {
			kvStore.maximizeMetaEta(readSet[i], txEntry.getCTS());
		} else {
			//put a tombstoned entry in KVStore???
			//or leave it blank???
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
        	kvStore.put(writeSetInStore[i], txEntry.getCTS(), txEntry.getPi(),
        			writeSet[i]->v.valuePtr, writeSet[i]->v.valueLength);
        } else {
        	kvStore.putNew(writeSet[i], txEntry.getCTS(), txEntry.getPi());
        	writeSet[i] = 0; //prevent txEntry destructor from freeing the KVLayout pointer
        }
    }
    return true;
}

bool
Validator::write(KLayout& k, uint64_t &vPrevEta) {
	DSSNMeta meta;
	kvStore.getMeta(k, meta);
	vPrevEta = meta.pStampPrev;
	return true;

	KVLayout *kv;
	bool found = kvStore.getValue(k, kv);
	if (found && !kv->isTombstone()) {
		return true;
	}
	return false;
}

bool
Validator::read(KLayout& k, KVLayout *&kv) {
	//FIXME: This read can happen concurrently while conclude() is
	//modifying the KVLayout instance.
	bool found = kvStore.getValue(k, kv);
	if (found && !kv->isTombstone()) {
		return true;
	}
	return false;
}

bool
Validator::updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry) {
	return peerInfo.update(cts, peerId, eta, pi, txEntry);
}

bool
Validator::insertConcludeQueue(TxEntry *txEntry) {
	return concludeQueue.push(txEntry);
}


bool
Validator::validateLocalTx(TxEntry& txEntry) {
    //calculate local eta and pi
    updateTxEtaPi(txEntry);

    if (txEntry.isExclusionViolated()) {
        txEntry.setTxState(TxEntry::TX_ABORT);
    } else {
        txEntry.setTxState(TxEntry::TX_COMMIT);
    }
    txEntry.setTxCIState(TxEntry::TX_CI_CONCLUDED);

    return true;
};

void
Validator::validateDistributedTxs(int worker) {
    /* Scheme B3
    while (true) {
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = activeTxSet[worker].begin(); itr != activeTxSet[worker].end(); ++itr) {
            TXEntry *txEntry = itr;

            if (txEntry->getTxState() == TXEntry::TX_PENDING 
                    && txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT) {

                //normal case first try:

                //calculate local ETA and PI
                updateTxEtaPi(*txEntry);

                //send PEER_SSN_INFO to Tx peers.
                //FIXME: SendTxPeerSSNInfo(txEntry);

                txEntry->setTXCiState(TXEntry::TX_CI_INPROGRESS);
            }

            if (txEntry->getTxState() == TXEntry::TX_PENDING) {
                uint64_t waitingTime = currentTime() - txEntry->getCTS();
                if (waitingTime > abortThreshold) {
                    txEntry->setTxState(TXEntry::TX_ABORT);
                } else if (waitingTime > alertThreshold) {
                    //such tx should be waiting for the peer's SSN info
                    assert (txEntry->getTxCiState() == TXEntry::TX_CI_INPROGRESS);

                    //request PEER_SSN_INFO from Tx peers. (blocking/non-blocking)
                    //FIXME: RequestTxPeerSSNInfo(txEntry);
                    //
                    //after the request, either here or the PEER_SSN_INFO handler
                    //will set the TxEntry's state to TX_ABORT or TX_COMMIT.
                }
            }

            if (txEntry->getTxState() == TXEntry::TX_ABORT
                    || (txEntry->getTxState() == TXEntry::TX_COMMIT) {

                //FIXME: the PEER_SSN_INFO RPC handler will conclude
                //conclude(*txEntry);

                activeTxSet[worker].remove(txEntry);
            }
        }
    }
    */
            


    /*  
    while (true) {
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            TXEntry *txEntry = itr;
            if (txEntry->getCTS() > currentTime()) {
                break; //no need to look further in the sorted queue
            }
            if (txEntry->getTxState() == TXEntry::TX_PENDING
                    && txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT) {
                //calculate local eta and pi
                updateTxEtaPi(*txEntry);

                //log the commit-intent for failure recovery
                //LATER

                //send non-blocking SEND_SSN_INFO IPC messages to tx peers
                //LATER

                //update state
                txEntry->setTxCIState(TXEntry::TX_CI_INPROGRESS);
            }
            if (txEntry->getTxState() == TXEntry::TX_PENDING) {
                //if tx takes too long, try to abort it
                if (txEntry->getCTS() - currentTime() > alertThreshold)
                    txEntry->setTxState(TXEntry::TX_ALERT);
            } else if (txEntry->getTxState() == TXEntry::TX_ALERT) {
                //send non-blocking REQUEST_SSN_INFO IPC to tx peers
            } else if (txEntry->getTxState() == TXEntry::TX_ABORT
                    || txEntry->getTxState() == TXEntry::TX_COMMIT) {
                //remove tx from reorder queue
                reorderQueue.remove(txEntry);

                //trigger conclusion, be it an abort or a commit
                conclude(*txEntry);

                //remove from active tx set if needed
                if (txEntry->getTxCIState() == TXEntry::TX_CI_TRANSIENT
                        || txEntry->getTxCIState() == TXEntry::TX_CI_INPROGRESS
                        || txEntry->getTxCIState() == TXEntry::TX_CI_CONCLUDED) {
                    activeTxSet.remove(txEntry);
                }
            }
        }
    }
    */
}

void
Validator::scheduleDistributedTxs() {
	TxEntry *txEntry;
	uint64_t lastCTS = 0;
    while (true) {
    	if ((txEntry = (TxEntry *)reorderQueue.try_pop(clock.getLocalTime()))) {
    		if (txEntry->getCTS() < lastCTS)
    			continue; //ignore this CI that is past a processed CI
    		while (!blockedTxSet.add(txEntry));
    	}
    }
}

void
Validator::serialize() {
    /*
     * This loop handles the DSSN serialization window critical section
     */
	bool hasEvent = true;
    while (!isUnderTest || hasEvent) {
    	hasEvent = false;

        // process due commit-intents on cross-shard transaction queue


    	/*
    	 * Henry: To dequeue from the skip list could lower performance of this critical section.
    	 * I propose we use a thread before this serializer thread to move the scheduled
    	 * CIs from the skip list to a boost spsc queue. The serializer dequeues CIs
    	 * from the spsc queue. Now whether this spsc queue is tied to the DM
    	 * or using a different spsc queue that only contains blocked CIs to tie to DM
    	 * is something to consider.
    	 */

        
        /* Scheme B3
        TXEntry *txEntry;
        // FIXME: do we need to wait for empty execution slot here?
        
        if (txEntry = blockedTxSet.findReadyTx()) {
            activeTxSet[worker].add(txEntry);
            blockedTxSet.remove(txEntry);
            continue;
        }

        //FIXME consider changing this to add itr one at a time.
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            txEntry = itr;
            if (txEntry->getCTS() > currentTime() ) {
                break;
            } else if (blockfactor = blockedTxSet.blocks(txEntry)) {
                blockedTxSet.add(txEntry, blockfactor);
            } else if (blockfactor = activeTxSet.blocks(txEntry)) {
                blockedTxSet.add(txEntry, blockfactor);
            } else {
                activeTxSet[worker].add(txEntry);
                // FIXME: Log the Commit Intent 
            }
        }
        */

        /*
        for (SkipList<std::vector<uint8_t>,TXEntry *>::iterator itr = reorderQueue.begin(); itr != reorderQueue.end(); ++itr) {
            TXEntry *txEntry = itr;
            if (txEntry->getCTS() > currentTime()) {
                break; //no need to look further in the sorted queue
            } else if (txEntry->getTxCIState() == TXEntry::TX_CI_QUEUED) {
                //check dependency on earlier transactions
                if (activeTxSet.depends(txEntry)) {
                    if (txEntry->getTxCIState() == TXEntry::TX_CI_QUEUED) {
                        txEntry->setTxCIState(TXEntry::TX_CI_WAITING);
                        blockedTxSet.add(txEntry);
                    }
                    continue;
                }
                if (blockedTxSet.depends(txEntry)) {
                    continue;
                }

                //schedule for validation as there is no dependency
                if (activeTxSet.add(txEntry)) {
                    txEntry->setTxCIState(TXEntry::TX_CI_TRANSIENT);
                    lastCTS = txEntry->getCTS();

                    //remove from blocked tx set if needed
                    if (blockedTxSet.contains(txEntry)
                        blockedTxSet.remove(txEntry);
                }
            }
        }
        */

        // process all commit-intents on local transaction queue
        TxEntry* txEntry;
        while (localTxQueue.try_pop(txEntry)) {
        	if (activeTxSet.blocks(txEntry)) {
        		localTxQueue.repush(txEntry); // re-enqueued as this tx may be unblocked later
        	} else {
        		/* There is no need to update activeTXs because this tx is validated
        		 * and concluded shortly. If the conclude() does through a queue and another
        		 * task, then we should add tx to active tx set here.
        		 */

        		// As local transactions can be validated in any order, we can set the CTS.
        		// Use lastCTS+1 as CTS. That has very little impact on cross-shard txs.
        		txEntry->setCTS(++lastCTS);

        		validateLocalTx(*txEntry);

        		conclude(*txEntry);
        	}
        	hasEvent = true;
        }

        while ((txEntry = blockedTxSet.findReadyTx(activeTxSet))) {
        	assert(activeTxSet.add(txEntry));
        	txEntry->setTxCIState(TxEntry::TX_CI_TRANSIENT);
        	hasEvent = true;
        }

        while (concludeQueue.try_pop(txEntry)) {
        	if (lastCTS < txEntry->getCTS())
        		lastCTS = txEntry->getCTS();
        	conclude(*txEntry);
        	hasEvent = true;
        }
    } //end while(true)
}

bool
Validator::conclude(TxEntry& txEntry) {
    /*
     * log the commit result of a local tx.?
     * log the commit/abort result of a distributed tx as its CI has been logged?
     */

	//record results and meta data
	if (txEntry.getTxState() == TxEntry::TX_COMMIT) {
		updateKVReadSetEta(txEntry);
		updateKVWriteSet(txEntry);
	}

	if (txEntry.getPeerSet().size() >= 1)
		activeTxSet.remove(&txEntry);

	return true;
}

void
Validator::sweep() {
	peerInfo.sweep();
}

} // end Validator class

