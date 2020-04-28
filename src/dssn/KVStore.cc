// Copyright (c) 2020 Futurewei Technologies Inc

#include "KVStore.h"
#include <hot/rowex/HOTRowex.hpp>

namespace DSSN
{
using HotKVType = hot::rowex::HOTRowex<DSSN::KVLayout*, HOTKeyExtractor>;

bool operator == (const KLayout &lhs, const KLayout &rhs)
{
    return (lhs.keyLength == rhs.keyLength && (memcmp(lhs.key.get(), rhs.key.get(), lhs.keyLength)==0));
}

KVStore::KVStore() {
	hotKVStore = new HotKVType();
	assert(hotKVStore);
}

KVLayout*
KVStore::preput(KVLayout &kvIn) {
	KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
	std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvOut->k.keyLength);
	kvOut->v.valueLength = kvIn.v.valueLength;
	kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
	std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
    kvOut->v.meta = kvIn.v.meta;
    kvOut->v.isTombstone = kvIn.v.isTombstone;
	return kvOut;
}

KVLayout *
KVStore::fetch(KLayout& k) {

	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		return ret.mValue;
	}
	return 0;
}

bool
KVStore::putNew(KVLayout *kv, uint64_t cts, uint64_t pi) {
	kv->meta().cStamp = kv->meta().pStamp = cts;
	kv->meta().pStampPrev = 0;
	kv->meta().sStampPrev = pi;
	kv->meta().sStamp = 0xffffffffffffffff;
	idx::contenthelpers::OptionalValue<DSSN::KVLayout*> ret = ((HotKVType *)hotKVStore)->upsert(kv);
	if (!ret.mIsValid)
		return false;
	return true;
}

bool
KVStore::put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength) {
	kv->meta().cStamp = kv->meta().pStamp = cts;
	kv->meta().pStampPrev = kv->meta().pStamp;
	kv->meta().sStampPrev = pi;
	kv->meta().sStamp = 0xffffffffffffffff;
	delete kv->v.valuePtr;
	kv->v.valueLength = valueLength;
	kv->v.valuePtr = valuePtr;
	return true;
}

// Obsoleted, reason in the header file
/*
bool
KVStore::getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength) {

        KVLayout *kv;
        if (getValue(k, kv)) {
            valuePtr = kv->v.valuePtr;
            valueLength = kv->v.valueLength;
            return true;
        }
        return false;
}

//Obsoleted, reason in the header file
bool
KVStore::getValue(KLayout& k, KVLayout *&kv) {

	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		kv = ret.mValue;
		return true;
	}
	kv = 0;
	return false;
}


//Obsoleted, reason in the header file
bool
KVStore::getMeta(KLayout& k, DSSNMeta &meta)
{
	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		meta = kv->meta();
		return true;
	}
	return false;
}
*/

/*
bool
KVStore::maximizeMetaEta(KVLayout *kv, uint64_t eta) {
	kv->meta().pStamp = std::max(eta, kv->meta().pStamp);;
	return true;
}
*/


bool
KVStore::remove(KLayout& k, DSSNMeta &meta) {
	HotKVType::KeyType key = (char *)k.key.get();

	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		kv->isTombstone(true);
		kv->meta(meta);
		return true;
	}
	return false;
}
} // end namespace
