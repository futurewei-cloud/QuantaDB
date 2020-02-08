/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "CountBloomFilter.h"
#include "MurmurHash3.h"

namespace DSSN {

inline bool
CountBloomFilter::add(const T *key, uint32_t size) {
     uint64_t idx1, idx2;
     createIndexesFromKey(key, size, &idx1, &idx2);
     if (counters[idx1] < 255 && counters[idx2] < 255) {
         counters[idx1]++;
         counters[idx2]++;
         return true;
     }
     return false;
}

inline bool
CountBloomFilter::remove(const T *key, uint32_t size) {
     uint64_t idx1, idx2;
     // assume that the key has actually been added
     createIndexesFromKey(key, size, &idx1, &idx2);
     if (counters[idx1] > 0 && counters[idx2] > 0) {
         counters[idx1]--;
         counters[idx2]--;
         return true;
     }
     return false;
}

inline bool
CountBloomFilter::contains(const T *key, uint32_t size) {
     uint64_t idx1, idx2;
     // assume that the key has actually been added
     createIndexesFromKey(key, size, &idx1, &idx2);
     return (counters[idx1] > 0 && counters[idx2] > 0);
}

inline void
CountBloomFilter::createIndexesFromKey(const T *key, uint32_t size, uint64_t *idx1, uint64_t *idx2) {
    std::array<uint64_t, 2> indexes;
    RAMCloud::MurmurHash3_x64_128(key, size, 0, indexes.data());
    *idx1 = indexes[0] % counters.size();
    *idx2 = indexes[1] % counters.size();
}

} // end CountBloomFilter class

