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

#ifndef ACTIVE_TX_SET_H
#define ACTIVE_TX_SET_H

#include "CountBloomFilter.h"
#include "TxEntry.h"

namespace QDB {

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
    CountBloomFilter<uint8_t> cbf;
    std::atomic<uint64_t> removedTxCount{0};
    std::atomic<uint64_t> addedTxCount{0};

    PUBLIC:
    // false if the key is failed to be added due to overflow
    bool add(TxEntry *txEntry);

    // for performance, the key is assumed to have been added
    bool remove(TxEntry *txEntry);

    bool blocks(TxEntry *txEntry);

    // for testing
    uint64_t getRemovedTxCount() { return removedTxCount; }
    uint64_t getCount() { return addedTxCount - removedTxCount; }

    // for testing
    bool clear() { return cbf.clear(); }

    ActiveTxSet() {}

}; // end ActiveTxSet class

} // end namespace QDB

#endif  /* ACTIVE_TX_SET_H */

