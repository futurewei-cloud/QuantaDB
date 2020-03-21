/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "Common.h"
#include "ActiveTxSet.h"
#include "BlockedTxSet.h"
#include "TxEntry.h"
#include "WaitQueue.h"
#include "KVStore.h"
#include "HashmapKVStore.h"
#include "PeerInfo.h"
#include "ConcludeQueue.h"
#include <boost/lockfree/queue.hpp>

namespace DSSN {

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
    PeerInfo peerInfo;
	ConcludeQueue concludeQueue;
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

    // used for read/write by coordinator
    bool read(KLayout& k, KVLayout *&kv);
    bool write(KLayout& k, uint64_t &vPrevEta);
    bool updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry);
    bool insertConcludeQueue(TxEntry *txEntry);
}; // end Validator class

} // end namespace DSSN

#endif  /* VALIDATOR_H */

