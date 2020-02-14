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

} // end TxEntry class

