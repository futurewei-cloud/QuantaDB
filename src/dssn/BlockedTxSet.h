
/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef BLOCKEDTXSET_H
#define BLOCKEDTXSET_H

#include "TxEntry.h"
#include "ActiveTxSet.h"

namespace DSSN {

const uint32_t SIZE = 1024;

/*
 * The class contains some due cross-shard CIs that are blocked by activeTxSet.
 *
 * It is implemented as a circular array of dependency bits, where a set dependency bit
 * represents that the current tx has dependency on a prior added tx, indicated by the bit position,
 * ahead of itself in the circular array. The bitmap representation enables a quick uint64_t
 * check on dependency on all transactions ahead.
 *
 * The circular array is maintained by a head, a waist, and a tail.
 * A removal of an item is possible between the head and the tail. When the head
 * points to a removed item, the head is advanced, not to go beyond the waist.
 * An add may advance the tail, not to hit the head, or may retract the tail, up to the waist,
 * or may replace a removed item between the waist and the tail.
 * The waist is advanced when pushed by the head.
 *
 * The consumer updates the head.
 * The producer updates the tail and the waist.
 *
 */
class BlockedTxSet {
	PROTECTED:


	//dependency matrix (DM), showing tx R depending on tx C
	uint64_t detailDependBits[SIZE][SIZE / 64];

	//summary dependency, showing tx R independent of all earlier txs queued
	uint64_t summaryDependBits[SIZE / 64];

	TxEntry* txs[SIZE];

	//indexes of the above arrays
	uint32_t head;
	uint32_t waist;
	uint32_t tail;

	inline bool isKeySetOverlapped(TxEntry *tx1, TxEntry *tx2);
	inline bool dependsOnEarlier(uint32_t idx) { return summaryDependBits[idx / 64] & (1 << (idx % 64)); }
	inline void removeDependency(uint32_t idx);

    PUBLIC:
    // return true if the CI is added successfully
	/// checks whether there is space inside DM, internally managing tail and waist
    bool add(TxEntry *txEntry);

    // return the CI that is not blocked by active tx set nor by any earlier CIs in DM
    /// checks ready CI between head and tail, internally managing head
    TxEntry* findReadyTx(ActiveTxSet &activeTxSet);

    BlockedTxSet();

}; 

} // end namespace DSSN

#endif  // BLOCKEDTXSET_H
