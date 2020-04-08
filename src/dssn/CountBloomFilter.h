/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef COUNT_BLOOM_FILTER_H
#define COUNT_BLOOM_FILTER_H

#include "Common.h"

namespace DSSN {

/**
 * Counting Bloom Filter
 *
 * It must protect itself from overflowing any counter.
 * For now, only use two hash functions.
 * It supports single incrementer and multi decrementers.
 *
 */
template <class T>
class CountBloomFilter {
    PROTECTED:
    std::atomic<T> *counters;
    CountBloomFilter& operator=(const CountBloomFilter&);
    uint32_t size = 65536; //number of counters
    uint32_t depth = 255; //max counter value allowed

    PUBLIC:
	CountBloomFilter();
	CountBloomFilter(uint32_t size, uint32_t depth);
	~CountBloomFilter();

    // false if the key is failed to be added due to overflow
    inline bool add(uint64_t hash);

    // for performance, the key is assumed to have been added
    inline bool remove(uint64_t hash);

    // test whether ok to add
    inline bool shouldNotAdd(uint64_t hash);

    // return the maximum of the bucket counts when there is a hit; otherwise, 0
    inline uint64_t hitCount(uint64_t hash);

    inline uint32_t countLimit() { return this->depth; }

    inline bool clear();

    inline void createIndexesFromKey(const uint8_t *key, uint32_t size, uint64_t *idx1, uint64_t *idx2);

}; // end CountBloomFilter class

} // end namespace DSSN

#endif  /* COUNT_BLOOM_FILTER_H */

