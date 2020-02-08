/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef COUNT_BLOOM_FILTER_H
#define COUNT_BLOOM_FILTER_H

#include "Common.h"

namespace DSSN {

typedef std::string T;

/**
 * Counting Bloom Filter
 *
 * It must protect itself from overflowing any counter.
 * For now, only use two hash functions.
 * It assumes one incrementer thread and one decrementer thread,
 * and those two can be the same one. That is, neither multiple incrementers nor multiple
 * decrementers are supported.
 *
 * LATER: may have caller to provide idx1 and idx2 if the caller pre-calculates and stores hash values
 */
class CountBloomFilter {
    PROTECTED:
    std::vector<std::atomic<uint8_t>> counters;

    PUBLIC:
    CountBloomFilter(uint32_t size) { counters.resize(size, 0); }
    ~CountBloomFilter();

    // false if the key is failed to be added due to overflow
    inline bool add(const T *key, uint32_t size);

    // for performance, the key is assumed to have been added
    inline bool remove(const T *key, uint32_t size);

    inline bool contains(const T *key, uint32_t size);

    inline void createIndexesFromKey(const T *key, uint32_t size, uint64_t *idx1, uint64_t *idx2);


}; // end CountBloomFilter class

} // end namespace DSSN

#endif  /* COUNT_BLOOM_FILTER_H */

