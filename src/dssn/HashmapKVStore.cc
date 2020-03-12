/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */
#include <atomic>
#include <queue>
#include <errno.h>
#include <time.h>
#include "HashmapKVStore.h"

namespace DSSN {

KVLayout* HashmapKVStore::preput(KVLayout &kvIn)
{
	KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
	std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvOut->k.keyLength);
	kvOut->v.valueLength = kvIn.v.valueLength;
	kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
	std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
	return kvOut;
}

bool HashmapKVStore::putNew(KVLayout *kv, uint64_t cts, uint64_t pi)
{
	kv->meta.cStamp = kv->meta.pStamp = cts; //cStamp is a volatile, signaling var; do it first
	kv->meta.pStampPrev = 0;
	kv->meta.sStampPrev = pi;
	kv->meta.sStamp = 0xffffffffffffffff;
    Element * elem = new Element(kv);
    elem_pointer<Element> lptr = my_hashtable->put(elem->key, elem);
    return lptr.ptr_ != NULL;
}

bool HashmapKVStore::put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength)
{
	kv->meta.cStamp = kv->meta.pStamp = cts; //cStamp is a volatile, signaling var; do it first
	kv->meta.pStampPrev = kv->meta.pStamp;
	kv->meta.sStampPrev = pi;
	kv->meta.sStamp = 0xffffffffffffffff;
    if (kv->v.valuePtr)
	    delete kv->v.valuePtr;
	kv->v.valueLength = valueLength;
	kv->v.valuePtr = valuePtr;
	return true;
}

KVLayout * HashmapKVStore::fetch(KLayout& k)
{
    const Element * elem;
    //char key[k.keyLength + 1];

    // XXX: could save this cpu cycles, if k.key.get() is null terminated
    // strncpy(key, (char *)k.key.get(), k.keyLength); key[k.keyLength] = 0;

    elem_pointer<Element> lptr = my_hashtable->get((char*)k.key.get());
    if ((elem = lptr.ptr_) != NULL) {
        return elem->kv;
    }
    return NULL;
}

bool HashmapKVStore::getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength)
{
    KVLayout * kv = fetch(k);

    if (kv == NULL) {
	    valueLength = 0;
        return false;
    }
	valuePtr = kv->v.valuePtr;
	valueLength = kv->v.valueLength;
	return true;
}

bool HashmapKVStore::getValue(KLayout& k, KVLayout *&kv)
{
    kv = fetch(k);
    return (kv != NULL);
}

bool HashmapKVStore::getMeta(KLayout& k, DSSNMeta &meta)
{
    KVLayout * kv = fetch(k);
    if (kv) {
	    meta = kv->meta;
		return true;
	}
	return false;
}

bool HashmapKVStore::maximizeMetaEta(KVLayout *kv, uint64_t eta) {
	kv->meta.pStamp = std::max(eta, kv->meta.pStamp);;
	return true;
}

bool HashmapKVStore::remove(KLayout& k, DSSNMeta &meta)
{
    KVLayout * kv = fetch(k);
    if (kv) {
		kv->isTombstone = true;
		kv->meta = meta;
		return true;
	}
	return false;
}

} // DSSN
