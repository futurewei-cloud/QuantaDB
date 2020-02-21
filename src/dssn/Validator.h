/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "Common.h"
#include "Object.h"
#include "HOTKV.h"
#include <tbb/tbb.h>
#include "ActiveTxSet.h"
#include "BlockedTxSet.h"
#include "TxEntry.h"
#include "WaitQueue.h"

namespace DSSN {
typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;
//typedef tbb::concurrent_queue<TxEntry*> WaitQueue;


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

    // all operations about tuple store
    HOTKV tupleStore;
    uint64_t getTupleEta(Object& object);
    uint64_t getTuplePi(Object& object);
    uint64_t getTuplePrevEta(Object& object);
    uint64_t getTuplePrevPi(Object& object);
    const std::string* getTupleValue(Object& object);
    bool maximizeTupleEta(Object& object, uint64_t eta);
    bool updateTuple(Object& object, TxEntry& txEntry);

    // all SSN data maintenance operations
    bool updateTxEtaPi(TxEntry& txEntry);
    bool updateReadSetTupleEta(TxEntry& txEntry);
    bool updateWriteSetTuple(TxEntry& txEntry);

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

