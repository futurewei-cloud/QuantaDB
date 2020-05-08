/* Copyright (c) 2020  Futurewei Technologies, Inc.
 * All rights are reserved.
 */
#pragma once

#include "Common.h"
#include "TxEntry.h"
#include "DLog.h"

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
    TxLog(bool); // for revovery mode

    //add to the log, where txEntry->getTxState() decides the handling within
    ///expected to be used for persisting the tx state then and the read and write sets
    ///expected to be used with cross-shard txs only
    /// recovery needs to log CIs of local txs also???
    bool add(TxEntry *txEntry);

    //return the last logged tx state: supposedly one of TX_PENDING, TX_ABORT, and TX_COMMIT.
    ///expected to be used for replying to peer's or coordinator's request for tx state.
    ///how long should TxLog keep the tx states?
    uint32_t getTxState(uint64_t cts);

    //obtain the first (non-concluded) commit-intent in the log
    ///the returned id is used for iterating through the non-concluded commit-intents
    ///the returned id has meaning internal to the class but is expected to be tx CTS
    ///the class may allocate peerSet and writeSet, caller responsible for destructing them?
    ///expected to be used for restart recovery
    bool getFirstPendingTx(uint64_t &idOut, DSSNMeta &meta, std::set<uint64_t> &peerSet, boost::scoped_array<KVLayout*> &writeSet);

    //obtain the next (non-concluded) commit-intent in the log after the one identified by id
    ///the id, which is considered to be the iterator internally, will be advanced
    ///the class may allocate peerSet and writeSet, caller responsible for destructing them?
    ///expected to be used for restart recovery
    bool getNextPendingTx(uint64_t idIn, uint64_t &idOut, DSSNMeta &meta, std::set<uint64_t> &peerSet, boost::scoped_array<KVLayout*> &writeSet);

    private:
    // private struct
    typedef struct TxLogHeader {
        #define TX_LOG_SIG 0xA5A5F0F0
        uint32_t sig;   // signature
        uint32_t length;// log record size, include this header
    } TxLogHeader_t;

    // private variables
    #define TXLOG_DIR   "/tmp/txlog"
    #define TXLOG_CHUNK_SIZE (64*1024*1024)
    DLog<TXLOG_CHUNK_SIZE> *log;
}; // TxLog

} // end namespace DSSN
