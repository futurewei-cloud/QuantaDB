/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef ACTIVE_TX_SET_H
#define ACTIVE_TX_SET_H

#include "CountBloomFilter.h"
#include "TXEntry.h"

namespace DSSN {

/**
 * It is an approximate membership set of transactions undergoing validation.
 * Instead of using fine grain locks of tuples, using approximate membership
 * reduces the amount of locks at the cost of a small amount of false positives.
 *
 * LATER: may have caller to provide idx1 and idx2 if the caller pre-calculates and stores hash values
 */
class ActiveTxSet {
    PROTECTED:
    CountBloomFilter cbf;

    PUBLIC:
    ActiveTxSet(uint32_t size) { cbf = CountBloomFilter(size); }
    ~ActiveTxSet();

    // false if the key is failed to be added due to overflow
    inline bool add(TXEntry *txEntry);

    // for performance, the key is assumed to have been added
    inline bool remove(TXEntry *txEntry);

    inline bool depends(TXEntry *txEntry);

    inline void createIndexes(TXEntry *txEntry);

}; // end ActiveTxSet class

} // end namespace DSSN

#endif  /* ACTIVE_TX_SET_H */

