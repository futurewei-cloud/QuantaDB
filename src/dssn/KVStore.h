// Copyright (c) 2020 Futurewei Technologies Inc

#ifndef __KVSTORE_H__
#define __KVSTORE_H__

#include <memory>

namespace DSSN {

struct DSSNMeta {
	uint64_t pStamp; //eta
	uint64_t sStamp; //pi
	uint64_t pStampPrev; //eta of prev version
	uint64_t sStampPrev; //pi of prev version
	uint64_t cStamp; //creation time (or CTS)
	bool isTombStone;
	DSSNMeta() {
		cStamp = 0;
		pStampPrev = pStamp = 0;
		sStampPrev = sStamp = 0xffffffffffffffff;
		isTombStone = false;
	}
};

struct VLayout {
	uint32_t valueLength;
	uint8_t *valuePtr;
	DSSNMeta meta;
};

struct KLayout {
	uint32_t keyLength;
	union {
		uint8_t key[256];
		uint8_t keyHead[0];
	};
};

struct KVLayout {
	VLayout v; //fixed length, therefore placed first
	KLayout k; //could be of variable length, therefore placed next
};

/*
 * This class contains a k-v store whose structure is optimized for look-up by key.
 * The size of key is limited to 256B currently. The key is supposed to be
 * globally unique. That is, the key may be composed of tenant ID, table ID, tuple key, etc.
 * The value part is composed of the tuple value data and the DSSN meta data.
 * Since the tuple value data is variable in size, the class separates the storage of the
 * value and the storage of the pointer to the value storage.
 *
 * The class could encapsulate the PelagoDB or a persistent store. When it is backed by
 * such a persistent store, the class serves as an in-memory cache,
 * supporting latest-version point query of tuples
 * while the PelagoDB would support snapshot reads, range queries, and point queries of
 * cold tuples.
 *
 * The class is expected to be used by the critical section, serialize(). Therefore,
 * it is designed to minimize memory copy of key data and value data.
 */
class KVStore {
    /*
     * The caller provides the object in the RPC message.
     * The kvIn should have the k.keyHead at the start of the key data
     * and v.valuePtr correctly point to the temp memory of the RPC message.
     * This class will produce a new KVLayout object and possibly allocate value memory with the
     * v.valuePtr pointing to it.
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
     *
     */
    bool getMeta(const KLayout& k, DSSNMeta *meta);

    /*
     * The caller prepares k.keyLength and k.key. Returns the valuePtr and valueLength.
     */
    bool getValue(const KLayout& k, uint8_t *valuePtr, uint32_t &valueLength);

    /*
     * The caller prepares k.keyLength and k.key. Returns the pointer to VLayout.
     */
    bool getValue(const KLayout& k, VLayout *v);

}; //end class KVStore
} //end namespace DSSN

#endif /* __KVSTORE_H__ */
