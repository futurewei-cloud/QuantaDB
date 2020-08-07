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

void *clhash_random;
bool HashmapKVStore::hash_inited = 0;

KVLayout* HashmapKVStore::preput(KVLayout &kvIn)
{
    //Fixme: need to allocate from a garbage-collecting pool and report any failure
    KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
    if (kvOut == NULL)
        return NULL;
    std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvOut->k.keyLength);
    kvOut->v.valueLength = kvIn.v.valueLength;
    if (kvIn.v.valueLength > 0) {
        //Fixme: need to allocate from "persistent memory" and report any failure
    	kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
    	if (kvOut->v.valuePtr == NULL)
    	    return NULL;
    	std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
    }
    kvOut->v.meta = kvIn.v.meta;
    kvOut->v.isTombstone = kvIn.v.isTombstone;
    return kvOut;
}

bool HashmapKVStore::putNew(KVLayout *kv, __uint128_t cts, uint64_t pi)
{
    kv->meta().cStamp = kv->meta().pStamp = cts >> 64;
    kv->meta().pStampPrev = 0;
    kv->meta().sStampPrev = pi;
    kv->meta().sStamp = (uint64_t) -1; //not overwritten yet
    if (kv->v.valuePtr == NULL || kv->v.valueLength == 0)
    	kv->v.isTombstone = true;
    elem_pointer<KVLayout> lptr = my_hashtable->put(kv->getKey(), kv);
    return lptr.ptr_ != NULL;
}

bool HashmapKVStore::put(KVLayout *kv, __uint128_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength)
{
    kv->meta().cStamp = kv->meta().pStamp = cts >> 64;
    kv->meta().pStampPrev = kv->meta().pStamp;
    kv->meta().sStampPrev = pi;
    kv->meta().sStamp = (uint64_t) -1; //not overwritten yet
    if (kv->v.valuePtr)
        delete kv->v.valuePtr;
    kv->v.valueLength = valueLength;
    kv->v.valuePtr = valuePtr;
    if (valuePtr == NULL || valueLength == 0)
    	kv->v.isTombstone = true;
    return true;
}

KVLayout * HashmapKVStore::fetch(KLayout& k)
{

    elem_pointer<KVLayout> lptr = my_hashtable->get(k);
    return lptr.ptr_;
}

} // DSSN
