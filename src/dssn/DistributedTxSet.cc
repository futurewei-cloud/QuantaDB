/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "DistributedTxSet.h"

namespace DSSN {

DistributedTxSet::DistributedTxSet() {
}

template <class T>
bool
DistributedTxSet::dependsOnEarlierTxs(T &cbf, TxEntry *txEntry, uint32_t &count) {
	count = 0;
	for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
		uint32_t tmp;
		if ((tmp = cbf.hitCount(txEntry->getReadSetHash()[i])) > count)
			count = tmp;
	}
	for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
		uint32_t tmp;
		if ((tmp = cbf.hitCount(txEntry->getWriteSetHash()[i])) > count)
			count = tmp;
	}
	return count > 0;
}

template <class T>
bool
DistributedTxSet::addToCBF(T &cbf, TxEntry *txEntry) {
    for (uint32_t i = 0; i < txEntry->getReadSetSize(); i++) {
        bool success = cbf.add(txEntry->getReadSetHash()[i]);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
            	cbf.remove(txEntry->getReadSetHash()[j]);
            }
            return false;
        }
    }
    for (uint32_t i = 0; i < txEntry->getWriteSetSize(); i++) {
        bool success = cbf.add(txEntry->getWriteSetHash()[i]);
        assert(success);
        if (!success) {
            // undo effects
            for (int j = i - 1; j >= 0; j--) {
            	cbf.remove(txEntry->getWriteSetHash()[j]);
            }
            for (uint32_t j = 0; j < txEntry->getReadSetSize(); j++) {
            	cbf.remove(txEntry->getReadSetHash()[j]);
            }
            return false;
        }
    }
	return true;
}

bool
DistributedTxSet::addToHotTxs(TxEntry *txEntry) {
	if (hotDependQueue.isFull())
		return false;
    if (addToCBF(hotDependCBF, txEntry)) {
    	assert(hotDependQueue.add(txEntry));
    	addedTxCount++;
    	return true;
    }
	return false;
}

bool
DistributedTxSet::addToColdTxs(TxEntry *txEntry) {
	if (coldDependQueue.isFull())
		return false;
    if (addToCBF(coldDependCBF, txEntry)) {
    	assert(coldDependQueue.add(txEntry));
    	addedTxCount++;
    	return true;
    }
	return false;
}

bool
DistributedTxSet::addToIndependentTxs(TxEntry *txEntry) {
	if (independentQueue.isFull())
		return false;
    if (addToCBF(independentCBF, txEntry)) {
    	assert(independentQueue.add(txEntry));
    	addedTxCount++;
    	return true;
    }
	return false;
}

bool
DistributedTxSet::add(TxEntry *txEntry) {
	uint32_t count;
	if (dependsOnEarlierTxs(hotDependCBF, txEntry, count)) {
		if (count >= hotDependCBF.countLimit())
			return false;
		return addToHotTxs(txEntry);
	}

	if (dependsOnEarlierTxs(coldDependCBF, txEntry, count)) {
		if (count >= hotThreshold)
			return addToHotTxs(txEntry);
		if (count >= coldDependCBF.countLimit())
			return false;
		return addToColdTxs(txEntry);
	}

	if (dependsOnEarlierTxs(independentCBF, txEntry, count)) {
		if (count >= independentCBF.countLimit())
			return false;
		return addToColdTxs(txEntry);
	}

	return addToIndependentTxs(txEntry);
}

TxEntry*
DistributedTxSet::findReadyTx(ActiveTxSet &activeTxSet) {

	//skip scanning if there is no change that matters
	if ((addedTxCount + removedTxCount + activeTxSet.getRemovedTxCount()) == activitySignature)
		return NULL;
	activitySignature = addedTxCount + removedTxCount + activeTxSet.getRemovedTxCount();

	TxEntry* txEntry;
	uint64_t it;

	txEntry = hotDependQueue.findFirst(it);
	if (txEntry
			&& txEntry->getCTS() < lastColdDependCTS
			&& txEntry->getCTS() < lastIndependentCTS
			&& !activeTxSet.blocks(txEntry)) {
		hotDependQueue.remove(it);
		removedTxCount++;
		return txEntry;
	}

	txEntry = coldDependQueue.findFirst(it);
	if (txEntry
			&& txEntry->getCTS() < lastIndependentCTS
			&& !activeTxSet.blocks(txEntry)) {
		lastColdDependCTS = txEntry->getCTS();
		coldDependQueue.remove(it);
		removedTxCount++;
		return txEntry;
	}

	txEntry = independentQueue.findFirst(it);
	do {
		if (txEntry && !activeTxSet.blocks(txEntry)) {
			lastIndependentCTS = txEntry->getCTS();
			independentQueue.remove(it);
			removedTxCount++;
			return txEntry;
		}
	} while ((txEntry = independentQueue.findNext(it)));

	return NULL;
}

} // end DistributedTxSet class

