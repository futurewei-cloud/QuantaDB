/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef ACTIVE_TX_SET_H
#define ACTIVE_TX_SET_H

#include "CountBloomFilter.h"
#include "TxEntry.h"

namespace DSSN {

/**
 * It is an approximate membership set of transactions undergoing validation.
 * Instead of using fine grain locks of tuples, using approximate membership
 * reduces the amount of locks at the cost of a small amount of false positives.
 * It supports single incrementer and multi decrementers.
 *
 * LATER: may have caller to provide idx1 and idx2 if the caller pre-calculates and stores hash values
 */
class ActiveTxSet {
    PROTECTED:
    CountBloomFilter cbf;
    uint64_t removedTxCount = 0;
    uint64_t addedTxCount = 0;

    PUBLIC:
    // false if the key is failed to be added due to overflow
    bool add(TxEntry *txEntry);

    // for performance, the key is assumed to have been added
    bool remove(TxEntry *txEntry);

    bool blocks(TxEntry *txEntry);

    inline uint64_t getRemovedTxCount() { return removedTxCount; }

    ActiveTxSet() { cbf.clear(); }

}; // end ActiveTxSet class

} // end namespace DSSN

#endif  /* ACTIVE_TX_SET_H */

