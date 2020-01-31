// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __KVINTERFACE_H__
#define __KVINTERFACE_H__

#include <string>
#include <memory>
#include <atomic>

namespace DSSN
{
    struct dssnMeta {
    	/*
        uint64_t pStamp;   //eta
        uint64_t pStampPrev; //pi
        uint64_t cStamp;   //current */ // by Henry
    	uint64_t pStamp; //eta
    	uint64_t sStamp; //pi
    	uint64_t pStampPrev; //eta of prev version
    	uint64_t sStampPrev; //pi of prev version
    	uint64_t cStamp; //creation time (or CTS)
    };
    //Key value format to be stored in the underlying data structure
    struct KeyValue {
        bool isTombStone;
        dssnMeta meta;
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
	    return (const size_t) kv->getKey().length();
	}
        const char* operator() (const KVType &kv) const {
	    return kv->getKey().c_str();
	}
    };

/**
 * This class defines the Key-Value(KV) interface
 */    
class KVInterface {
      public:
      virtual bool put(const std::string &key, const std::string &value, const dssnMeta& meta) = 0;
      virtual bool updateMeta(const std::string &searchKey, const dssnMeta& newMeta) = 0;
      virtual const std::string* get(const std::string &searchKey, dssnMeta& meta) const = 0;
      virtual bool getMeta(const std::string &searchKey, dssnMeta& meta) = 0;
      virtual void removeVersion(const std::string &searchKey, const dssnMeta& meta) = 0;
      virtual void remove(const std::string &searchKey) = 0;
      
};


}

#endif /* __KVINTERFACE_H__ */
