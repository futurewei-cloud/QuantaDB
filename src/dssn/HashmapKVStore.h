/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */
#pragma once
#include <atomic>
#include <queue>
#include <errno.h>
#include <time.h>
#include "hash_map.h"
#include "c_str_util_classes.h"
#include "KVStore.h"

#define	HASH_TABLE_TEMPLATE		Element, char *, uint64_t, hash_c_str, equal_to_c_str
#define	ROUND_DOWN(n,p)			(n & ~(p-1))
#define	ROUND_UP(n,p)			((n + p - 1) & ~(p - 1))

namespace DSSN {

#define MAX_KEYLEN  127

class Element {
    char key[MAX_KEYLEN + 1];
    KVLayout * kv;

    Element(KVLayout &ikv)
    {
        kv = &ikv;
        assert(kv->k.keyLength <= MAX_KEYLEN);
        strncpy(key, (const char *)kv->k.key.get(), MAX_KEYLEN);
    }  

    ~Element()
    {
        // delete kv;
    }
};

class HashmapKV : public KVStore
{
public:
	HashmapKV(uint32_t nbucket = 1024)
	{
		bucket_count = nbucket;
		my_hashtable = new hash_table<HASH_TABLE_TEMPLATE>(bucket_count);
	}

	~HashmapKV()
	{
        delete my_hashtable;
	}
    KVLayout* preput(KVLayout &kvIn);
    bool put(KVLayout& kv);
    bool getMeta(KLayout& k, DSSNMeta &meta);
    bool updateMeta(KLayout& k, DSSNMeta &meta);
    bool maximizeMetaEta(KLayout& k, uint64_t eta);
    bool getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength);
    bool getValue(KLayout& k, KVLayout *&kv);
    bool remove(KLayout& k, DSSNMeta &meta);

private:
	hash_table<HASH_TABLE_TEMPLATE> * my_hashtable;
	uint32_t            bucket_count;
};

} // DSSN
