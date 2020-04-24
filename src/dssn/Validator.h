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
#include "KVStore.h"
#include "HashmapKVStore.h"
#include "PeerInfo.h"
#include "ConcludeQueue.h"
#include <boost/lockfree/queue.hpp>
#include "SkipList.h"
#include "ClusterTimeService.h"
#include "DistributedTxSet.h"

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

    WaitList localTxQueue{1000001};
    SkipList reorderQueue;
    DistributedTxSet distributedTxSet;
    ActiveTxSet activeTxSet;
    PeerInfo peerInfo;
	ConcludeQueue concludeQueue;
    uint64_t alertThreshold = 1000; //LATER
    std::atomic<uint64_t> localTxCTSBase;
    ClusterTimeService clock;
    //LATER DependencyMatrix blockedTxSet;
    //KVStore kvStore;
    HashmapKVStore &kvStore;
    bool isUnderTest;

    // all SSN data maintenance operations
    bool updateTxPStampSStamp(TxEntry& txEntry); //Fixme: to be called by peer info sender also
    bool updateKVReadSetPStamp(TxEntry& txEntry);
    bool updateKVWriteSet(TxEntry& txEntry);

    // schedule SSN validation on distributed transactions
    /// move due CIs from reorderQueue into blockedTxSet
    void scheduleDistributedTxs();

    // serialization of commit-intent validation
    void serialize();

    // perform SSN validation on distributed transactions
    /// help cross-shard CIs in activeTxSet to exchange SSN info with peers
    void validateDistributedTxs(int worker);

    // perform SSN validation on a local transaction
    bool validateLocalTx(TxEntry& txEntry);

    // handle validation commit/abort conclusion
    /// move committed data into backing store and update meta data
    bool conclude(TxEntry& txEntry);

    // handle garbage collection
    void sweep();

    PUBLIC:
	//Validator(HashmapKVStore &kvStore);
	Validator(HashmapKVStore &kvStore, bool isTesting = false);
	~Validator();

    // used for tx RPC handlers
    /* The current design does not expect a write to reach the validator.
     * SSN validation still works properly as the serialize() would go through the
     * write set to retrieve the latest committed version for its sstamp and pstamp.
     *
     * As for read, RPC handler would return the cts, sstamp, pstamp, associated
     * value, and VLayout table id and offset to the caller, and those in turn would be conveyed
     * in the commit intent (whose RPC handler would insertTxEntry())
     * to enable the validator to locate the proper version of the read tuple to continue
     * the validation.
     *
     * For distributed tx, validator needs to exchange meta data with peering validators.
     * Upon receiving meta data from a peer, RPC handler would updatePeerInfo() and
     * insertConcludeQueue().
     */
    bool read(KLayout& k, KVLayout *&kv);
    bool insertTxEntry(TxEntry *txEntry);
    bool updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry);
    bool insertConcludeQueue(TxEntry *txEntry);

    // for unit testing, triggering a run of functions without using threads
    bool testRun();
}; // end Validator class

} // end namespace DSSN

#endif  /* VALIDATOR_H */

