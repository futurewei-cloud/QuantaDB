/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"
#include "MurmurHash3.h"

namespace DSSN {


TxEntry::TxEntry() {
    this->pi = std::numeric_limits<uint64_t>::max();
    this->eta = 0;
    this->txState = TX_PENDING;
    this->commitIntentState = TX_CI_UNQUEUED;
    this->cts = 0;
}

TxEntry::~TxEntry() {
	for (uint32_t i = 0; i < writeSet.size(); i++) {
		delete writeSet[i];
	}
	for (uint32_t i = 0; i < readSet.size(); i++) {
		delete readSet[i];
	}
}

bool
TxEntry::insertReadSet(KVLayout* kv) {
	readSet.push_back(kv);
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	readSetHash.push_back((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

bool
TxEntry::insertWriteSet(KVLayout* kv) {
	writeSet.push_back(kv);
	uint64_t indexes[2];
	RAMCloud::MurmurHash3_x64_128(kv->k.key.get(), kv->k.keyLength, 0, indexes);
	writeSetHash.push_back((indexes[0] << 32) | (indexes[1] & 0xffffffff));
	return true;
}

} // end TxEntry class

