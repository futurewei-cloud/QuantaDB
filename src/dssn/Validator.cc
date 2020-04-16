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

Validator::Validator(HashmapKVStore &_kvStore) : kvStore(_kvStore) {
}

Validator::~Validator() {
}

void
Validator::start() {
	localTxCTSBase = clock.getLocalTime();

    // Henry: may need to use TBB to pin the threads to specific cores LATER
    std::thread( [=] { serialize(); });
    std::thread( [=] { sweep(); });
    std::thread( [=] { scheduleDistributedTxs(); });
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
    		txEntry.setPi(std::min(txEntry.getPi(), kv->meta().sStamp));
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
    		txEntry.setEta(std::max(txEntry.getEta(), kv->meta().pStamp));
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
	                readSet[i]->meta().pStamp = std::max(txEntry.getCTS(), readSet[i]->meta().pStamp);;
		} else {
			//Fixme: put a tombstoned entry in KVStore???
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

bool
Validator::read(KLayout& k, KVLayout *&kv) {
	//FIXME: This read can happen concurrently while conclude() is
	//modifying the KVLayout instance.
        kv = kvStore.fetch(k);
        return (kv!=NULL && !kv->isTombstone());
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
Validator::scheduleDistributedTxs() {
	TxEntry *txEntry;
    do {
    	if ((txEntry = (TxEntry *)reorderQueue.try_pop(clock.getLocalTime()))) {
    		if (txEntry->getCTS() < localTxCTSBase)
    			continue; //ignore this CI that is past a processed CI
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
        	assert(activeTxSet.add(txEntry));
        	txEntry->setTxCIState(TxEntry::TX_CI_TRANSIENT);
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

	txEntry.setTxCIState(TxEntry::TX_CI_FINISHED);
	return true;
}

void
Validator::sweep() {
	peerInfo.sweep();
}

} // end Validator class

