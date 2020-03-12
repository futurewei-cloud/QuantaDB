// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __KVSTORE_H__
#define __KVSTORE_H__

#include "Common.h"
#include <boost/scoped_array.hpp>

namespace DSSN {

struct DSSNMeta {
	uint64_t pStamp; //eta
	uint64_t sStamp; //pi
	uint64_t pStampPrev; //eta of prev version
	uint64_t sStampPrev; //pi of prev version
	volatile uint64_t cStamp; //creation time (or CTS)
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
    VLayout() {
        valuePtr = NULL;
    }
};

struct KLayout {
	uint32_t keyLength = 0;
	boost::scoped_array<uint8_t> key;
	KLayout() {}
	explicit KLayout(uint32_t keySize) : keyLength(keySize), key(new uint8_t[keySize+1]) { bzero(key.get(), keySize+1);}
};

struct KVLayout {
	VLayout v;
	KLayout k;
	DSSNMeta meta;
	bool isTombstone = false;

	explicit KVLayout(uint32_t keySize) : k(keySize) {}
	uint8_t* getKey() { return k.key.get(); }
	uint32_t getKeyLength() {return k.keyLength; }
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
    bool getMeta(KLayout& k, DSSNMeta &meta);

    /*
     * Update eta with the input eta and existing eta, whichever is larger.
     */
    bool maximizeMetaEta(KVLayout *kv, uint64_t eta);

    /*
     * The caller prepares k.keyLength and k.key. Returns the valuePtr and valueLength.
     */
    bool getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength);

    /*
     * The caller prepares k.keyLength and k.key. Returns the pointer to VKLayout.
     */
    bool getValue(KLayout& k, KVLayout *&kv);

    /*
     * The caller prepares k.keyLength and k.key.
     * The KV tuple is marked tomb-stoned and would be removed lazily.
     */
    bool remove(KLayout& k, DSSNMeta &meta);

}; //end class KVStore
} //end namespace DSSN

#endif /* __KVSTORE_H__ */
