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

#ifndef COUNT_BLOOM_FILTER_H
#define COUNT_BLOOM_FILTER_H

#include "Common.h"

namespace QDB {

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
    uint32_t size{65536}; //number of counters
    uint32_t depth{255}; //max counter value allowed

    PUBLIC:
	CountBloomFilter();
	CountBloomFilter(uint32_t size, uint32_t depth);
	~CountBloomFilter();

    // false if the key is failed to be added due to overflow
    bool add(uint64_t hash);

    // for performance, the key is assumed to have been added
    bool remove(uint64_t hash);

    // test whether ok to add
    bool shouldNotAdd(uint64_t hash);

    // return the maximum of the bucket counts when there is a hit; otherwise, 0
    uint64_t hitCount(uint64_t hash);

    uint32_t countLimit() { return this->depth; }

    bool clear();

    void createIndexesFromKey(const uint8_t *key, uint32_t size, uint64_t *idx1, uint64_t *idx2);

}; // end CountBloomFilter class

} // end namespace QDB

#endif  /* COUNT_BLOOM_FILTER_H */

