/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "BlockedTxSet.h"

namespace DSSN {

BlockedTxSet::BlockedTxSet() {
	head = waist = tail = 0;
	std::memset(detailDependBits, 0, sizeof(detailDependBits));
	std::memset(summaryDependBits, 0, sizeof(summaryDependBits));
	std::memset(txs, 0, sizeof(txs));
}

inline bool
BlockedTxSet::isKeySetOverlapped(boost::scoped_array<uint64_t> &set1, uint32_t size1,
		boost::scoped_array<uint64_t> &set2, uint32_t size2) {
	uint32_t i, j;
	i = j = 0;
	while (i < size1 && j < size2) {
		if (set1[i] == set2[j])
			return true;
		if (set1[i] < set2[j])
			i++;
		else
			j++;
	}
	return false;
}

inline bool
BlockedTxSet::isTxKeySetOverlapped(TxEntry *tx1, TxEntry *tx2) {
	// DSSN dependency check: read-write, write-write, and write-read dependencies
	/// comparing exact key values would be more precise,
	/// but it is more efficient comparing their hash values
	/// assume hash values are already sorted (presumably during the tx entry instantiation)

	if (isKeySetOverlapped(tx1->getReadSetHash(), tx1->getReadSetSize(),
			tx2->getWriteSetHash(), tx2->getWriteSetSize())
			|| isKeySetOverlapped(tx1->getWriteSetHash(), tx1->getWriteSetSize(),
					tx2->getWriteSetHash(), tx2->getWriteSetSize())
					|| isKeySetOverlapped(tx1->getWriteSetHash(), tx1->getWriteSetSize(),
							tx2->getReadSetHash(), tx2->getReadSetSize()))
		return true;
	return false;
}

bool
BlockedTxSet::add(TxEntry *txEntry) {
	//try to retract the tail towards the waist
	if (waist != tail) {
		do {
			if (txs[(tail - 1) % SIZE] != NULL)
				break; //cannot retract past an unprocessed tx
			tail = (tail - 1) % SIZE;
		} while (tail != waist);
	}

	uint32_t head = this->head;

	//try to advance the waist towards the tail if the head is pushing waist
	///do not advance past an unprocessed independent tx
	///because that could be available spot for insertion soon
	if (waist == head && waist != tail) {
		do {
			if (dependsOnEarlier(waist) && !dependsOnEarlier((waist + 1) % SIZE))
				break;
			waist = (waist + 1) % SIZE;
		} while (tail != waist);
	}

	//determine whether current tx depends on previous txs
	for (uint32_t j = head; j != tail; j = (j + 1) % SIZE) {
		if (txs[j] == NULL)
			continue; //skip invalid/removed tx
		//when the slot is empty, detailDependBits and summaryDependBits should have been initialized
		if (isTxKeySetOverlapped(txEntry, txs[j])) {
			detailDependBits[tail][j / 64] |= (1 << (j % 64));
			summaryDependBits[tail / 64] |= (1 << (tail % 64));
		}
	}

	//insert tx in an empty spot between waist and tail if the tx is independent
	if (!dependsOnEarlier(tail) && waist != tail) {
		for (uint32_t idx = waist; idx != tail; idx = (idx + 1) % SIZE) {
			if (txs[idx] == NULL) {
				txs[idx] = txEntry;
				addedTxCount++;
				return true;
			}
		}
	}

	if ((tail + 1) % SIZE == head)
		return false; //because there is no room

	txs[tail] = txEntry;
	addedTxCount++;
	tail = (tail + 1) % SIZE;
    return true;
}

void
BlockedTxSet::removeDependency(uint32_t idx) {
	for (uint32_t j = (idx + 1) % SIZE; j != tail; j = (j + 1) % SIZE) {
		if (summaryDependBits[j / 64] & (1 << (j % 64))) {
			uint64_t tmp = (detailDependBits[j][idx / 64] &= ~(1 << (idx % 64)));
			if (tmp != 0)
				continue; //because there won't be effect to summaryDependBits
			for (uint32_t k = 0; k < SIZE / 64; k++) {
				tmp |= detailDependBits[j][k];
			}
			if (tmp == 0)
				summaryDependBits[j / 64] &= ~(1 << (j % 64));
		}
	}
}

TxEntry*
BlockedTxSet::findReadyTx(ActiveTxSet &activeTxSet) {
	if (head != waist) {
		do {
			if (txs[head] != NULL)
				break;
			head = (head + 1) % SIZE;
		} while (head != waist);
	}

	//skip scanning if there is no change that matters
	if (activeTxSet.getRemovedTxCount() == activeTxSetSignature
			&& (addedTxCount + removedTxCount) == blockedTxSetSignature)
		return NULL;
	blockedTxSetSignature = addedTxCount + removedTxCount;
	activeTxSetSignature = activeTxSet.getRemovedTxCount(); //must be placed before blocks(..) check

	//tail could be changed by another thread, but a retracted tail would have no effect here
	//as those txs associated with the retraction must have been removed
	uint32_t tail = this->tail; //tail could be changed by another thread
	if (head == tail) //empty
		return NULL;
	uint32_t idx = head;
	TxEntry *txEntry;
	do {
		txEntry = txs[idx];
		if (txEntry && !dependsOnEarlier(idx) && !activeTxSet.blocks(txEntry)) {
			txs[idx] = 0; //indicate that current tx is removed
			removeDependency(idx); //update the later txs not to depend on the current tx
			removedTxCount++;
			return txEntry;
		}
		idx = (idx + 1) % SIZE;
	} while (idx != tail);

	return NULL;
}

} // end BlockedTxSet class

