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
     * The caller prepares k.keyLength and k.key. Returns toe pointers to the valuePtr and valueLength.
     */
    bool getValue(const KLayout& k, uint8_t *valuePtr, uint32_t &valueLength);
}; //end class KVStore
} //end namespace DSSN

#endif /* __KVSTORE_H__ */
