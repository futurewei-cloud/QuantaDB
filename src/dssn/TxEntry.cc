/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"
#include "MurmurHash3.h"

namespace DSSN {

TxEntry::TxEntry(uint32_t readSetSize, uint32_t writeSetSize) {
	this->pi = std::numeric_limits<uint64_t>::max();
	this->eta = 0;
	this->txState = TX_PENDING;
	this->commitIntentState = TX_CI_UNQUEUED;
	this->cts = 0;
	this->writeSetSize = writeSetSize;
	this->readSetSize = readSetSize;
	this->readSetIndex = this->writeSetIndex = 0;
	this->writeSet.reset(new KVLayout *[writeSetSize]);
	this->writeSetHash.reset(new uint64_t[writeSetSize]);
	this->writeSetInStore.reset(new KVLayout *[writeSetSize]);
	this->readSet.reset(new KVLayout *[readSetSize]);
	this->readSetHash.reset(new uint64_t[readSetSize]);
	this->readSetInStore.reset(new KVLayout *[readSetSize]);
}


TxEntry::~TxEntry() {
	for (uint32_t i = 0; i < writeSetSize; i++) {
		delete writeSet[i];
	}
	for (uint32_t i = 0; i < readSetSize; i++) {
		delete readSet[i];
	}
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

