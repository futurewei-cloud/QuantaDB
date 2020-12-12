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

#include "KVStore.h"
//#include <hot/rowex/HOTRowex.hpp>

namespace QDB
{
/*using HotKVType = hot::rowex::HOTRowex<QDB::KVLayout*, HOTKeyExtractor>;
 */

bool operator == (const KLayout &lhs, const KLayout &rhs)
{
    return (lhs.keyLength == rhs.keyLength && (memcmp(lhs.key.get(), rhs.key.get(), lhs.keyLength)==0));
}

KVStore::KVStore() {
    /*
	hotKVStore = new HotKVType();
	assert(hotKVStore);
    */
}

KVLayout*
KVStore::preput(KVLayout &kvIn) {
    /*
    KVLayout* kvOut = new KVLayout(kvIn.k.keyLength);
    if (kvOut == NULL)
        return NULL;
    std::memcpy((void *)kvOut->k.key.get(), (void *)kvIn.k.key.get(), kvOut->k.keyLength);
    kvOut->v.valueLength = kvIn.v.valueLength;
    if (kvIn.v.valueLength > 0) {
        kvOut->v.valuePtr = new uint8_t[kvIn.v.valueLength];
        if (kvOut->v.valuePtr == NULL)
            return NULL;
        std::memcpy((void *)kvOut->v.valuePtr, (void *)kvIn.v.valuePtr, kvIn.v.valueLength);
    }
    kvOut->v.meta = kvIn.v.meta;
    kvOut->v.isTombstone = kvIn.v.isTombstone;
    return kvOut;
    */
    return NULL;
}

KVLayout *
KVStore::fetch(KLayout& k) {
    /*
	HotKVType::KeyType key = (char *)k.key.get();
	idx::contenthelpers::OptionalValue<KVLayout*> ret = ((HotKVType *)hotKVStore)->lookup(key);
	if (ret.mIsValid) {
		return ret.mValue;
	}
    */
	return 0;
}

bool
KVStore::putNew(KVLayout *kv, uint64_t cts, uint64_t pi) {
    /*
	kv->meta().cStamp = kv->meta().pStamp = cts;
	kv->meta().pStampPrev = 0;
	kv->meta().sStampPrev = pi;
	kv->meta().sStamp = 0xffffffffffffffff;
	if (kv->v.valuePtr == NULL || kv->v.valueLength == 0)
		kv->v.isTombstone = true;
	idx::contenthelpers::OptionalValue<QDB::KVLayout*> ret = ((HotKVType *)hotKVStore)->upsert(kv);
	if (!ret.mIsValid)
		return false;
    */
	return true;
}

bool
KVStore::put(KVLayout *kv, uint64_t cts, uint64_t pi, uint8_t *valuePtr, uint32_t valueLength) {
    /*
	kv->meta().cStamp = kv->meta().pStamp = cts;
	kv->meta().pStampPrev = kv->meta().pStamp;
	kv->meta().sStampPrev = pi;
	kv->meta().sStamp = 0xffffffffffffffff;
	delete kv->v.valuePtr;
	kv->v.valueLength = valueLength;
	kv->v.valuePtr = valuePtr;
	if (valuePtr == NULL || valueLength == 0)
		kv->v.isTombstone = true;
    */
	return true;
}

} // end namespace
