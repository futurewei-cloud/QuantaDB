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

#pragma once
#include <atomic>
#include <queue>
#include <errno.h>
#include <time.h>
#include "clhash.h"
#include "hash_map.h"
#include "KVStore.h"

#define	ROUND_DOWN(n,p)			(n & ~(p-1))
#define	ROUND_UP(n,p)			((n + p - 1) & ~(p - 1))

namespace QDB {

extern void *clhash_random;

struct HashKLayout{
    uint32_t operator()(const KLayout &k) { return clhash(clhash_random, (const char*)k.key.get(), k.keyLength); }
};

#define MAX_KEYLEN  127

class HashmapKVStore : public KVStore
{
public:
    HashmapKVStore(uint32_t nbucket = DEFAULT_BUCKET_COUNT)
    {
        bucket_count = nbucket;
        my_hashtable = new hash_table<KVLayout, KLayout, VLayout, HashKLayout>(bucket_count, false/*non-lossy*/);
        if (!hash_inited) {
            clhash_random =  get_random_key_for_clhash(uint64_t(0x23a23cf5033c3c81),uint64_t(0xb3816f6a2c68e530));
            hash_inited = true;
        }
    }

    ~HashmapKVStore()
    {
        delete my_hashtable;
    }
    KVLayout* preput(KVLayout &kvIn);
    bool putNew(KVLayout *kv, __uint128_t cts, uint64_t pi);
    bool put(KVLayout *kv, __uint128_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength);
    KVLayout * fetch(KLayout& k);
    // the following three functions will be obsoleted
    //bool getMeta(KLayout& k, DSSNMeta &meta);
    //bool getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength);
    //bool getValue(KLayout& k, KVLayout *&kv);
    bool remove(KLayout& k, DSSNMeta &meta);
    uint32_t get_evict_count() { return my_hashtable->get_evict_count(); }
    uint32_t get_avg_elem_iter_len() { return my_hashtable->get_avg_elem_iter_len(); }
private:
    hash_table<KVLayout, KLayout, VLayout, HashKLayout> * my_hashtable;
    uint32_t            bucket_count;
    static bool hash_inited;
};

} // QDB
