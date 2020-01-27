// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __KVINTERFACE_H__
#define __KVINTERFACE_H__

#include <string>
#include <memory>

namespace DSSN
{
    //Key value format to be stored in the underlying data structure
    struct KeyValue {
        bool isTombStone;
        uint64_t meta;
        std::string key;
        std::string value;
        std::string& getKey() {
	    return key;
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
      virtual bool put(const std::string &key, const std::string &value, const uint64_t meta) = 0;
      virtual const std::string& get(const std::string &searchKey, uint64_t* meta) = 0;
      virtual void removeVersion(const std::string &searchKey, const uint64_t meta) = 0;
      virtual void remove(const std::string &searchKey) = 0;
      
};


}

#endif /* __KVINTERFACE_H__ */
