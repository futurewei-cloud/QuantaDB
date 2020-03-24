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
	//overflow protection here is not necessary if the caller uses shoudlNotAdd() properly
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
CountBloomFilter::shouldNotAdd(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % BFSize, idx2 = (hash & 0xffffffff) % BFSize;
    // Return true on any one of the two conditions
    /// the first: the key is in the BF
    /// the second: if the key is added to the BF, a BF counter will overflow
    /// The second condition ensures add() to return true
    return ((counters[idx1] > 0 && counters[idx2] > 0) || counters[idx1] == 255 || counters[idx2] == 255);
}

} // end CountBloomFilter class

