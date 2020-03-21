/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "CountBloomFilter.h"
#include "MurmurHash3.h"

namespace DSSN {

CountBloomFilter::CountBloomFilter() {
	clear();
}

bool
CountBloomFilter::clear() {
    for (uint32_t i = 0; i < BFSize; i++) {
        counters[i] = 0;
    }
    return true;
}

bool
CountBloomFilter::add(uint64_t hash) {
     uint64_t idx1 = (hash >> 32) % BFSize, idx2 = (hash & 0xffffffff) % BFSize;
     if (counters[idx1] < 255 && counters[idx2] < 255) {
         counters[idx1]++;
         counters[idx2]++;
         return true;
     }
     return false;
}

bool
CountBloomFilter::remove(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % BFSize, idx2 = (hash & 0xffffffff) % BFSize;
     // assume that the key has actually been added
     if (counters[idx1] > 0 && counters[idx2] > 0) {
         counters[idx1]--;
         counters[idx2]--;
         return true;
     }
     return false;
}

bool
CountBloomFilter::contains(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % BFSize, idx2 = (hash & 0xffffffff) % BFSize;
     // assume that the key has actually been added
     return (counters[idx1] > 0 && counters[idx2] > 0);
}

} // end CountBloomFilter class

