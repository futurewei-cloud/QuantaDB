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

KVLayout* HashmapKV::preput(KVLayout &kvIn)
{
	KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
	kvOut->k.keyLength = kvIn.k.keyLength;
	std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvIn.k.keyLength);
	kvOut->v.valueLength = kvIn.v.valueLength;
	kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
	std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
	return kvOut;
}

bool HashmapKV::put(KVLayout& kv)
{
    Element * elem = new Element(kv);
    elem_pointer<Element> lptr = my_hashtable->put(elem->key, elem);
    return lptr.ptr != NULL;
}

bool HashmapKV::getMeta(KLayout& k, DSSNMeta &meta)
{
    return true;
}

bool HashmapKV::updateMeta(KLayout& k, DSSNMeta &meta)
{
    return true;
}

bool HashmapKV::maximizeMetaEta(KLayout& k, uint64_t eta)
{
    return true;
}

bool HashmapKV::getValue(KLayout& k, uint8_t *&valuePtr, uint32_t &valueLength)
{
    return true;
}

bool HashmapKV::getValue(KLayout& k, KVLayout *&kv)
{
    return true;
}

bool HashmapKV::remove(KLayout& k, DSSNMeta &meta)
{
    return true;
}

} // DSSN
