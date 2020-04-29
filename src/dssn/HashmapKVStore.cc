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
    KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
    std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvOut->k.keyLength);
    kvOut->v.valueLength = kvIn.v.valueLength;
    kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
    std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
    kvOut->v.meta = kvIn.v.meta;
    kvOut->v.isTombstone = kvIn.v.isTombstone;
    return kvOut;
}

bool HashmapKVStore::putNew(KVLayout *kv, uint64_t cts, uint64_t pi)
{
    kv->meta().cStamp = kv->meta().pStamp = cts;
    kv->meta().pStampPrev = 0;
    kv->meta().sStampPrev = pi;
    kv->meta().sStamp = (uint64_t) -1; //not overwritten yet
    if (kv->v.valuePtr == NULL || kv->v.valueLength == 0)
    	kv->v.isTombstone = true;
    elem_pointer<KVLayout> lptr = my_hashtable->put(kv->getKey(), kv);
    return lptr.ptr_ != NULL;
}

bool HashmapKVStore::put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength)
{
    kv->meta().cStamp = kv->meta().pStamp = cts;
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
