/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include "Common.h"
#include "TxEntry.h"

namespace DSSN {
/**
 * This class provides transaction logging service for storage node restart recovery.
 *
 * The validator uses this class to persist essential tx info, retrieve persisted tx info,
 * and detect not-yet-validated commit intents upon recovery.
 *
 * The class is responsible for maintaining and cleaning the logged tx info.
 */
class TxLog {
    public:
    TxLog();

    //add to the log, where txEntry->getTxState() decides the handling within
    ///expected to be used for persisting the tx state then and the read and write sets
    ///expected to be used with cross-shard txs only
    /// recovery needs to log CIs of local txs also???
    bool add(TxEntry *txEntry);

    //return the last logged tx state: supposedly one of TX_PENDING, TX_ABORT, and TX_COMMIT.
    uint32_t getTxState(uint64_t cts);

    //obtain the first (non-concluded) commit-intent in the log
    ///the returned id is used for iterating through the non-concluded commit-intents
    ///the returned id has meaning internal to the class but is expected to be tx CTS
    ///the class may allocate readSet and writeSet, caller responsible for destructing them
    bool getFirstPendingTx(uint64_t &idOut, boost::scoped_array<KVLayout> &readSet, boost::scoped_array<KVLayout> &writeSet);

    //obtain the next (non-concluded) commit-intent in the log after the one identified by id
    ///the id, which is considered to be the iterator internally, will be advanced
    ///the class may allocate readSet and writeSet, caller responsible for destructing them
    bool getNextPendingTx(uint64_t idIn, uint64_t &idOut, boost::scoped_array<KVLayout> &readSet, boost::scoped_array<KVLayout> &writeSet);

    private:

}; // TxLog

} // end namespace DSSN
