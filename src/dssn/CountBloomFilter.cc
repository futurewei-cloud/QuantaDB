/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "CountBloomFilter.h"
#include "MurmurHash3.h"

namespace DSSN {

template <class T>
CountBloomFilter<T>::CountBloomFilter() {
	counters = new std::atomic<T>[size];
	clear();
}

template <class T>
CountBloomFilter<T>::CountBloomFilter(uint32_t _size, uint32_t _depth) {
	size = _size;
	depth = _depth;
	counters = new std::atomic<T>[size];
	clear();
}

template <class T>
CountBloomFilter<T>::~CountBloomFilter() {
	delete counters;
}

template <class T>
bool
CountBloomFilter<T>::clear() {
    for (uint32_t i = 0; i < size; i++) {
        counters[i] = 0;
    }
    return true;
}

template <class T>
bool
CountBloomFilter<T>::add(uint64_t hash) {
	//overflow protection here is not necessary if the caller uses shoudlNotAdd() properly
	uint64_t idx1 = (hash >> 32) % size, idx2 = (hash & 0xffffffff) % size;
	if (counters[idx1] < depth && counters[idx2] < depth) {
		counters[idx1]++;
		counters[idx2]++;
		return true;
	}
	return false;
}

template <class T>
bool
CountBloomFilter<T>::remove(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % size, idx2 = (hash & 0xffffffff) % size;
     // assume that the key has actually been added
     if (counters[idx1] > 0 && counters[idx2] > 0) {
         counters[idx1]--;
         counters[idx2]--;
         return true;
     }
     return false;
}

template <class T>
bool
CountBloomFilter<T>::shouldNotAdd(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % size, idx2 = (hash & 0xffffffff) % size;
    // Return true on any one of the two conditions
    /// the first: the key is in the BF
    /// the second: if the key is added to the BF, a BF counter will overflow
    /// The second condition ensures add() to return true
    return ((counters[idx1] > 0 && counters[idx2] > 0) || counters[idx1] >= depth || counters[idx2] >= depth);
}

template <class T>
uint64_t
CountBloomFilter<T>::hitCount(uint64_t hash) {
    uint64_t idx1 = (hash >> 32) % size, idx2 = (hash & 0xffffffff) % size;
    if (counters[idx1] > 0 && counters[idx2] > 0)
    	return (std::max((uint64_t)counters[idx1], (uint64_t)counters[idx2]));
    return 0;
}


template class CountBloomFilter<uint8_t>;
template class CountBloomFilter<uint16_t>;
template class CountBloomFilter<uint32_t>;

} // end CountBloomFilter class

