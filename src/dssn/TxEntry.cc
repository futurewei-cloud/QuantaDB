/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"
#include "MurmurHash3.h"

namespace DSSN {

TxEntry::TxEntry(uint32_t _readSetSize, uint32_t _writeSetSize) {
	sstamp = std::numeric_limits<uint64_t>::max();
	pstamp = 0;
	txState = TX_PENDING;
	commitIntentState = TX_CI_UNQUEUED;
	cts = 0;
	writeSetSize = _writeSetSize;
	readSetSize = _readSetSize;
	readSetIndex = this->writeSetIndex = 0;
	writeSet.reset(new KVLayout *[writeSetSize]);
	writeSetHash.reset(new uint64_t[writeSetSize]);
	writeSetInStore.reset(new KVLayout *[writeSetSize]);
	readSet.reset(new KVLayout *[readSetSize]);
	readSetHash.reset(new uint64_t[readSetSize]);
	readSetInStore.reset(new KVLayout *[readSetSize]);
	for (uint32_t i = 0; i < writeSetSize; i++)
		writeSet[i] = writeSetInStore[i] = NULL;
	for (uint32_t i = 0; i < readSetSize; i++)
		readSet[i] = readSetInStore[i] = NULL;
}


TxEntry::~TxEntry() {
	//readWriteSet[i] and read/writeSetInStore[i] are managed by KV store - do not free here
	//scoped_array of KVLayout pointers is supposed to be freed implicitly
}

bool
TxEntry::insertReadSet(KVLayout* kv, uint32_t i) {
	readSet[i] = kv;
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	readSetHash[i] = ((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

bool
TxEntry::insertWriteSet(KVLayout* kv, uint32_t i) {
	writeSet[i] = kv;
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	writeSetHash[i] = ((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

} // end TxEntry class

