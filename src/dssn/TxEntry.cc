/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */


#include "TxEntry.h"

namespace DSSN {


TxEntry::TxEntry() {
    this->pi = std::numeric_limits<uint64_t>::max();
    this->eta = 0;
    this->txState = TX_PENDING;
    this->commitIntentState = TX_CI_UNQUEUED;
    this->cts = 0;
}

TxEntry::~TxEntry() {
	for (uint32_t i = 0; i < writeSet.size(); i++)
		delete writeSet[i];
	for (uint32_t i = 0; i < readSet.size(); i++)
		delete readSet[i];
}

} // end TxEntry class

