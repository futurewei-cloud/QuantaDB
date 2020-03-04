// Copyright (c) 2020 Futurewei Technologies Inc

#include "KVStore.h"
#include <hot/rowex/HOTRowex.hpp>

namespace DSSN
{
using HotKVType = hot::rowex::HOTRowex<DSSN::KVLayout*, HOTKeyExtractor>;
KVStore::KVStore() {
	hotKVStore = new HotKVType();
	assert(hotKVStore);
}

KVLayout*
KVStore::preput(KVLayout &kvIn) {
	KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
	kvOut->k.keyLength = kvIn.k.keyLength;
	std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvIn.k.keyLength);
	kvOut->v.valueLength = kvIn.v.valueLength;
	kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
	std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
	return kvOut;
}

bool
KVStore::put(KVLayout& kv) {
	idx::contenthelpers::OptionalValue<DSSN::KVLayout*> ret = ((HotKVType *)hotKVStore)->upsert(&kv);
	if (ret.mIsValid == true && ret.mValue != &kv) {
		KVLayout* oldkv = ret.mValue;
		delete oldkv->v.valuePtr;
		//assert(ret.mValue);
	}
	return true;
}

bool
KVStore::getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength) {

	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		valuePtr = kv->v.valuePtr;
		valueLength = kv->v.valueLength;
		return true;
	}
	return false;
}

bool
KVStore::getMeta(KLayout& k, DSSNMeta &meta)
{
	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		meta = kv->meta;
		return true;
	}
	return false;
}

bool
KVStore::updateMeta(KLayout& k, DSSNMeta &meta) {
	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		kv->meta = meta;
		return true;
	}
	return false;
}

bool
KVStore::remove(KLayout& k, DSSNMeta &meta) {
	HotKVType::KeyType key = (char *)k.key.get();

	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		KVLayout* kv = ret.mValue;
		kv->isTombStone = true;
		kv->meta = meta;
		return true;
	}
	return false;
}
} // end namespace
