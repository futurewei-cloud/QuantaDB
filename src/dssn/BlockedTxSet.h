
/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef BLOCKEDTXSET_H
#define BLOCKEDTXSET_H

#include "TxEntry.h"

namespace DSSN {

/*
 * The class contains some due cross-shard CIs that are blocked by activeTxSet.
 *
 * It is implemented as a circular array of dependency bits, where a set dependency bit
 * represents that the current tx has dependency on another tx, indicated by the bit position,
 * ahead of itself in the circular array. The bitmap representation enables a quick uint64_t
 * check on dependency on all transactions ahead.
 *
 * The circular array is maintained by a head and a tail. add() to the tail and remove() from head.
 *
 * It has space for add() when the head has not caught up with the tail and when the last
 * the tx to be added would not have non-zero dependency bits while there is at least one
 * already added tx that has no non-zero dependency bits.
 *
 */
class BlockedTxSet {
    PUBLIC:
    // return true if the CI is added successfully
    bool add(TxEntry *txEntry) {return true;}

    // for performance, the key is assumed to have been added
    bool remove(TxEntry *txEntry) {return true;}

    bool blocks(TxEntry *txEntry) {return true;}

    TxEntry* findReadyTx() {return NULL;}

    BlockedTxSet() {};

}; 

} // end namespace DSSN

#endif  // BLOCKEDTXSET_H
