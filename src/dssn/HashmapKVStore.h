/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
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

namespace DSSN {

extern void *clhash_random;

struct HashKLayout{
    uint32_t operator()(const KLayout &k) { return clhash(clhash_random, (const char*)k.key.get(), k.keyLength); }
};

#define MAX_KEYLEN  127

class HashmapKVStore : public KVStore
{
public:
    HashmapKVStore(uint32_t nbucket = 1024)
    {
        bucket_count = nbucket;
        my_hashtable = new hash_table<KVLayout, KLayout, VLayout, HashKLayout>(bucket_count);
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
    bool putNew(KVLayout *kv, uint64_t cts, uint64_t pi);
    bool put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength);
    KVLayout * fetch(KLayout& k);
    // the following three functions will be obsoleted
    //bool getMeta(KLayout& k, DSSNMeta &meta);
    //bool getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength);
    //bool getValue(KLayout& k, KVLayout *&kv);
    bool remove(KLayout& k, DSSNMeta &meta);

private:
    hash_table<KVLayout, KLayout, VLayout, HashKLayout> * my_hashtable;
    uint32_t            bucket_count;
    static bool hash_inited;
};

} // DSSN
