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

#ifndef __KVINTERFACE_H__
#define __KVINTERFACE_H__

#include <string>
#include <memory>
#include <atomic>

namespace QDB
{
    struct DSSNMeta {
    	uint64_t pStamp; //eta
    	uint64_t sStamp; //pi
    	uint64_t pStampPrev; //eta of prev version
    	uint64_t sStampPrev; //pi of prev version
    	uint64_t cStamp; //creation time (or CTS)
    	DSSNMeta() {
    	    cStamp = 0;
    	    pStampPrev = pStamp = 0;
    	    sStampPrev = sStamp = 0xffffffffffffffff;
    	}
    };
    //Key value format to be stored in the underlying data structure
    struct KeyValue {
        bool isTombStone;
        DSSNMeta meta;
        std::atomic_flag slock = ATOMIC_FLAG_INIT;
        std::string key;
        std::string value;
        std::string& getKey() {
	    return key;
	}
        void lock() {
	    while (slock.test_and_set(std::memory_order_acquire)) {}
	}
        void unlock() {
	    slock.clear(std::memory_order_release);
	}
    };

    //The versioned key value format to be stored in the underlying data structure
    struct KeyValueVersioned {
        KeyValue kv;
        std::unique_ptr<KeyValue> prevVersion;
    };

    //The helper structure to extract key from the stored key value
    template<typename KVType>
    struct KeyExtractor {
        typedef KVType KeyType;
	
        size_t getKeyLength(const KVType &kv) const {
	    if (kv != 0) {
	        return (const size_t) kv->getKey().length();
	    }
	    return 0;
	}
        const char* operator() (const KVType &kv) const {
	    static char s = 0;
	    if (kv != 0) {
	        return kv->getKey().c_str();
	    }
	    return &s;
	}
    };

/**
 * This class defines the Key-Value(KV) interface
 */    
class KVInterface {
      public:
      virtual bool put(const std::string &key, const std::string &value, const DSSNMeta& meta) = 0;
      virtual bool updateMeta(const std::string &searchKey, const DSSNMeta& newMeta) = 0;
      virtual const std::string* get(const std::string &searchKey, DSSNMeta& meta) const = 0;
      virtual bool getMeta(const std::string &searchKey, DSSNMeta& meta) = 0;
      virtual void removeVersion(const std::string &searchKey, const DSSNMeta& meta) = 0;
      virtual void remove(const std::string &searchKey) = 0;
      
};


}

#endif /* __KVINTERFACE_H__ */
