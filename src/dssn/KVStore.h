// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __KVSTORE_H__
#define __KVSTORE_H__

#include <memory>
#include <boost/scoped_array.hpp>

namespace DSSN {

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
	uint32_t valueLength;
	union {
		uint8_t *valuePtr;
		uint64_t offsetToValue; //assume 64-bit system, matching pointer size
	};
	DSSNMeta meta;
	bool isTombStone = false;
};

struct KLayout {
	uint32_t keyLength;
	union {
		uint32_t offsetToKey; //LATER to uint64_t
		boost::scoped_array<uint8_t> key;
	};

	explicit KLayout(uint32_t keySize) : keyLength(0), key(new uint8_t[keySize]) {}
};

struct KVLayout {
	VLayout v;
	KLayout k;
	uint8_t dataKeyAndValue[0];

	explicit KVLayout(uint32_t keySize) : k(keySize) {}
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
    /*
     * THe routine is intended to be used by a routine (say, RPC handler) prior to serialize() in
     * the transaction validation pipeline.
     *
     * Its purpose to have the KV store prepares its internal memory for the KV tuple so
     * that there would be minimal data copy within serialize().
     *
     * Whatever memory is allocated by the class will be freed by the class.
     *
     * The caller provides the object in the RPC message.
     * The kvIn should have the k.offsetToKey at the start of the key data
     * and v.offsetToValue correctly point to the value data.
     * This class will fill kvOut with k.key pointing to a key data copy
     * and v.valuePtr pointing to a value data copy.
     */
    bool preput(const KVLayout &kvIn, KVLayout *kvOut);

    /*
     * The key and value will be directly (i.e., without reformatting) copied into the search tree,
	 * Prior to this call, there should have been a preput() call to allocate value memory so that
	 * the v.valuePtr is to point to the value memory.
	 * Internally the KVStore will free the value memory if the v.valuePtr is to be changed.
     */
    bool put(const KVLayout& kv);

    /*
     * The caller provides k.keyLength and k.key. Upon successful return, meta points to the meta data.
     */
    bool getMeta(const KLayout& k, DSSNMeta *meta);

    /*
     * The caller provides k.keyLength and k.key. Upon successful return, meta points to the meta data.
     */
    bool updateMeta(const KLayout& k, DSSNMeta &meta);

    /*
     * The caller prepares k.keyLength and k.key. Returns the valuePtr and valueLength.
     */
    bool getValue(const KLayout& k, uint8_t *valuePtr, uint32_t &valueLength);

    /*
     * The caller prepares k.keyLength and k.key. Returns the pointer to VLayout.
     */
    bool getValue(const KLayout& k, VLayout *v);

    /*
     * The caller prepares k.keyLength and k.key.
     * The KV tuple is marked tomb-stoned and would be removed lazily.
     */
    bool remove(const KLayout& k);

}; //end class KVStore
} //end namespace DSSN

#endif /* __KVSTORE_H__ */
