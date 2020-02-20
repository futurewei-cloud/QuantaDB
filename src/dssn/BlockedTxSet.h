
/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef BLOCKEDTXSET_H
#define BLOCKEDTXSET_H

#include "TxEntry.h"

namespace DSSN {

class BlockedTxSet {
    PUBLIC:
    // false if the key is failed to be added due to overflow
    bool add(TxEntry *txEntry) {return true;}

    // for performance, the key is assumed to have been added
    bool remove(TxEntry *txEntry) {return true;}

    bool blocks(TxEntry *txEntry) {return true;}

    TxEntry* findReadyTx() {return NULL;}

    BlockedTxSet();

}; 

} // end namespace DSSN

#endif  // BLOCKEDTXSET_H
