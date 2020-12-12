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

#include <atomic>
#include <queue>
#include <errno.h>
#include <time.h>
#include "HashmapKVStore.h"
#include "Logger.h"

using namespace RAMCloud;

namespace QDB {

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
    /*
     * Fixme! XXX
     * Normally, in a production system, a KV put should always be successful. Here we use pmemhash KV for
     * its speed. Pmemhash KV either works in lossy mode where put() would always be successful, or in
     * non-lossy mode where put() may fail when the designated bucket was full.
     * We can't work with lossy mode. So we added an check below and exit if put failed.
     */
    if (lptr.ptr_ == NULL) {
        RAMCLOUD_LOG(ERROR,"pmemhash bucket full\n");
        exit(1);
    }
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

} // QDB
