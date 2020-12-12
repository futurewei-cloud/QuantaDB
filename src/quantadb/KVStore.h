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

#ifndef __KVSTORE_H__
#define __KVSTORE_H__

#include "Common.h"
#include "MemStreamIo.h"
#include <boost/scoped_array.hpp>

namespace QDB {

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

struct VLayout {
	uint32_t valueLength = 0;
	union {
		uint8_t *valuePtr;
		uint64_t offsetToValue; //assume 64-bit system, matching pointer size
	};
	DSSNMeta meta;
	bool isTombstone = false;
    VLayout() {
        valuePtr = NULL;
    }

    inline uint32_t serializeSize()
    {
        return sizeof(valueLength) + valueLength + sizeof(meta) + sizeof(isTombstone); 
    }

    inline void serialize( outMemStream & out )
    {
        out.write(&valueLength, sizeof(valueLength));
        out.write(valuePtr, valueLength);
        out.write(&meta, sizeof(meta));
        out.write(&isTombstone, sizeof(isTombstone));
    }

    inline void deSerialize( inMemStream & in )
    {
        in.read(&valueLength, sizeof(valueLength));
        if (valueLength > 0) {
            valuePtr = (uint8_t*)malloc(valueLength);
            in.read(valuePtr, valueLength);
        } else 
            valuePtr = NULL;
        in.read(&meta, sizeof(meta));
        in.read(&isTombstone, sizeof(isTombstone));
    }
};

struct KLayout {
	uint32_t keyLength = 0;
	boost::scoped_array<uint8_t> key;

    friend bool operator==(const KLayout &lhs, const KLayout &rhs);
	KLayout() {}
	explicit KLayout(uint32_t keySize) : keyLength(keySize), key(new uint8_t[keySize+1]) { bzero(key.get(), keySize+1);}

    inline uint32_t serializeSize()
    {
        return sizeof(keyLength) + keyLength;
    }

    inline void serialize( outMemStream & out )
    {
        out.write(&keyLength, sizeof(keyLength));
        out.write(key.get(), keyLength);
    }

    inline void deSerialize( inMemStream & in )
    {
        in.read(&keyLength, sizeof(keyLength));
        key.reset(new uint8_t[keyLength+1]);
        in.read(key.get(), keyLength);
        key.get()[keyLength] = 0; // null termination
    }
};

bool operator == (const KLayout &lhs, const KLayout &rhs);

struct KVLayout {
	VLayout v;
	KLayout k;


	explicit KVLayout(uint32_t keySize) : k(keySize) { }

	inline DSSNMeta& meta() { return v.meta; }
	inline void meta(const DSSNMeta &meta) { v.meta = meta; }
	inline bool isTombstone() { return v.isTombstone; }
	inline void isTombstone(const bool tomb) { v.isTombstone = tomb; }

	inline KLayout& getKey() { return k; }
	inline VLayout& getVLayout() { return v; }

    inline uint32_t serializeSize()
    {
        return v.serializeSize() + k.serializeSize();
    }

    inline void serialize( outMemStream & out )
    {
        v.serialize ( out );
        k.serialize ( out );
    }
    inline void deSerialize( inMemStream & in )
    {
        v.deSerialize ( in );
        k.deSerialize ( in );
    }
};

//The helper structure to extract key from the stored key value
template<typename KVType>
struct HOTKeyExtractor {
	typedef KVType KeyType;

	size_t getKeyLength(const KVType &kv) const {
		if (kv != 0) {
			return (const size_t) kv->k.keyLength;
		}
		return 0;
	}

	const char* operator() (const KVType &kv) const {
		static char s = 0;
		if (kv != 0) {
			return (char *)kv->k.key.get();
		}
		return &s;
	}
};

/*
 * This class contains a k-v store whose structure is optimized for using it
 * with minimal key data copy and value data copy outside the critical section
 * of DSSN, namely serialize().
 *
 * The k-v store's implementation may change over time. It is intended to
 * be backed by RAM and then later by PelagoDB. Therefore, it is intended to encapsulate
 * the underlying backing store(s).
 *
 * The key is supposed to be globally unique.
 * That is, the key may be composed of tenant ID, table ID, tuple key, etc.
 * The value part is composed of the data and meta data.
 * Since the data is variable in size, the class separates the storage of the
 * value and the storage of the pointer to the data.
 *
 */
class KVStore {
	PRIVATE:

	// Pointer to the underlying HOT data structure
	void* hotKVStore;

    PUBLIC:

	KVStore();

    /*
     * THe routine is intended to be used by a routine (say, RPC handler) prior to serialize() in
     * the transaction validation pipeline.
     *
     * Its purpose to have the KV store prepares its internal memory for the KV tuple so
     * that there would be minimal data copy within serialize().
     * It is to transfer KV tuple from non-KVStore-managed memory into KVStore-managed memory.
     * Whatever memory is allocated by the KVStore class will be freed by the class.
     *
     * The kvIn should have k.key and v.valuePtr point to RPC-managed memory.
     * This class will return a new object with k.key pointing to caller-managed memory
     * and v.valuePtr pointing to KVStore-managed memory.
     * Note that the KV tuple is not indexed nor search-able in KV store yet.
     */
    KVLayout* preput(KVLayout &kvIn);

    /*
     * The caller provides k.keyLength and k.key.
     * If the tuple exists in KV store, the stored KVLayout pointer is returned,
     * enabling the caller to access the KV memory in KV store directly.
     */
    KVLayout* fetch(KLayout& kIn);

    /*
     * The key and value will be directly (i.e., without reformatting) copied into the search tree,
	 * Prior to this call, there should have been a preput() call to allocate value memory so that
	 * the v.valuePtr is to point to the value memory.
	 * Internally the KVStore will free the value memory if the v.valuePtr is to be changed.
     */
    bool putNew(KVLayout *kv, uint64_t cts, uint64_t pi);
    bool put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength);


    /*
     * The caller provides k.keyLength and k.key. Upon successful return, meta points to the meta data.
     */
    /* Make this obsolete, as the only way to get the meta is to get the KVLayout then access the
     * meta field. Due to the multi-threaded nature of the underlying design, it's possible for
     * back to back invocation of getMeta and getValue(k,kv) to access different kv, even though the 
     * k is the same.*/
    //bool getMeta(KLayout& k, DSSNMeta &meta);

    /*
     * Update eta with the input eta and existing eta, whichever is larger.
     */
    // Make this obsolete, as this can be done by the KVLayout itself, don't have be in KVStore.
    //bool maximizeMetaEta(KVLayout *kv, uint64_t eta);

    /*
     * The caller prepares k.keyLength and k.key. Returns the valuePtr and valueLength.
     */
    // Make this obsolete as well, the same reason for obsoleting the getMeta(k,meta)
    //bool getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength);

    /*
     * The caller prepares k.keyLength and k.key. Returns the pointer to VKLayout.
     */
    // Make this obsolete as well, the same reason for obsoleting the getMeta(k,meta)
    //bool getValue(KLayout& k, KVLayout *&kv);

    /*
     * The caller prepares k.keyLength and k.key.
     * The KV tuple is marked tomb-stoned and would be removed lazily.
     */
    bool remove(KLayout& k, DSSNMeta &meta);

}; //end class KVStore
} //end namespace QDB

#endif /* __KVSTORE_H__ */
