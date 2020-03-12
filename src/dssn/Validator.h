/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "Common.h"
#include "Object.h"
#include "ActiveTxSet.h"
#include "BlockedTxSet.h"
#include "TxEntry.h"
#include "WaitQueue.h"
#include "KVStore.h"
#include "HashmapKVStore.h"

namespace DSSN {
typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;


/**
 * Supposedly one Validator instance per storage node, to handle DSSN validation.
 * It contains a pool of worker threads. They monitor the cross-shard
 * commit-intent (CI) queue and the local commit-intent queue and drain the commit-intents
 * (CI) from the queues. Each commit-intent is subject to DSSN validation.
 *
 */
class Validator {
    PROTECTED:

    WaitQueue localTxQueue;
    ActiveTxSet activeTxSet;
    BlockedTxSet blockedTxSet;
    uint64_t alertThreshold = 1000; //LATER
    uint64_t lastCTS = 0;
    bool isUnderTest = false;
    //LATER DependencyMatrix blockedTxSet;
    //KVStore kvStore;
    HashmapKVStore kvStore;

    // all SSN data maintenance operations
    bool updateTxEtaPi(TxEntry& txEntry);
    bool updateKVReadSetEta(TxEntry& txEntry);
    bool updateKVWriteSet(TxEntry& txEntry);

    // used for read/write by coordinator
    /// upon successful return, memory is allocated and pointed to by valuePtr
    /// the caller should free the memory
    bool read(KLayout& k, KVLayout *&kv, uint8_t *&valuePtr);
    bool write(KLayout& k, uint64_t &vPrevEta);

    // serialization of commit-intent validation
    void serialize();

    // perform SSN validation on distributed transactions
    void validateDistributedTxs(int worker);

    // perform SSN validation on a local transaction
    bool validateLocalTx(TxEntry& txEntry);

    // handle validation commit/abort conclusion
    bool conclude(TxEntry& txEntry);

    PUBLIC:
    // start threads and work
    void start();
}; // end Validator class

} // end namespace DSSN

#endif  /* VALIDATOR_H */

