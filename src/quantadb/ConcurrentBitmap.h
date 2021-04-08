/* Copyright 2021 Futurewei Technologies, Inc.
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

#pragma once
#include <cstdint>

namespace QDB {

class ConcurrentBitmap {
 public:
    ConcurrentBitmap() {
       uint64_t numUint64 =  (defaultBitmapSize>>6) + 1;
       bitmapArray = new uint64_t[numUint64]();
       bitmapArraySize = numUint64;
    };
    /*
    ConcurrentBitmap(uint64_t size) {
        uint64_t numUint64 =  (size>>6) + 1;
	bitmapArray = new uint64_t[numUint64]();
        bitmapArraySize = numUint64;
    }
    */
    static inline uint64_t getBitLocation(uint64_t hash) {
        return (hash % ConcurrentBitmap::defaultBitmapSize);
    }
    bool isSet(uint64_t hash) {
        uint8_t pos;
	uint64_t value;
	uint64_t bitLoc = ConcurrentBitmap::getBitLocation(hash);
	value = getSlotValue(bitLoc);
	pos = getBitPos(bitLoc);
	return (value >> pos) & 0x1;
    }
    bool add(uint64_t hash) {
        return set(hash);
    }
    bool remove(uint64_t hash) {
        return unset(hash);
    }
    bool shouldNotAdd(uint64_t hash) {
        return isSet(hash);
    }
    bool set(uint64_t hash) {
        uint64_t setBit = 1;
        uint64_t oldValue;
	uint64_t bitLoc = ConcurrentBitmap::getBitLocation(hash);
	uint8_t pos = getBitPos(bitLoc);
	uint64_t slot = getSlot(bitLoc);
	bool result;
	setBit = setBit << pos;
	
	do {
	    oldValue = bitmapArray[slot];

	    if (oldValue & setBit) {
	        return false;
	    }
	    result = __sync_bool_compare_and_swap(&bitmapArray[slot],
						  oldValue,
						  (oldValue | setBit));
	} while(!result);
	return true;
    }
    
    bool unset(uint64_t hash) {
        uint64_t unsetBit = 1;
        uint64_t oldValue;
	uint64_t bitLoc = ConcurrentBitmap::getBitLocation(hash);
	uint8_t pos = getBitPos(bitLoc);
	uint64_t slot = getSlot(bitLoc);
	unsetBit = unsetBit << pos;
	bool result;
	
	do {
	    oldValue = bitmapArray[slot];
	    if (oldValue & unsetBit) {
	      result = __sync_bool_compare_and_swap(&bitmapArray[slot],
						    oldValue,
						    (oldValue & ~unsetBit));
	    } else {
	        return false;
	    }
	} while(!result);
	return true;
    }
    bool clear() {
      for(uint64_t i = 0; i <bitmapArraySize; i++) {
	  bitmapArray[i] = 0;
      }
      return true;
    }
    bool isClean() {
      uint64_t val = 0;
      for(uint64_t i = 0; i <bitmapArraySize; i++) {
	  val |= bitmapArray[i];
      }
      return (val == 0);
    }
 private:
    inline uint64_t getSlotValue(uint64_t bitLoc) {
        uint64_t slot, value;
	slot = getSlot(bitLoc);
	return bitmapArray[slot];
    }
    inline uint64_t getSlot(uint64_t bitLoc) {
        uint64_t slot;
	slot = bitLoc % bitmapArraySize;
	return slot;
    }

    inline uint8_t getBitPos(uint64_t bitLoc) {
        return (uint8_t)bitLoc & 0x3F;
    }
    //const uint64_t defaultBitmapSize = 10*1024*1024;
    static const uint64_t defaultBitmapSize = 100*1024*1024;
    uint64_t* bitmapArray;
    uint64_t bitmapArraySize;
};

}
