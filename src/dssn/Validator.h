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
#include "DSSNService.h"

namespace DSSN {

//forward declaration to resolve interdependency
class DSSNService;
class PeerInfo;

/**
 * Supposedly one Validator instance per storage node, to handle DSSN validation.
 * It contains someone worker threads. They monitor the cross-shard
 * commit-intent (CI) queue and the local commit-intent queue and drain the commit-intents
 * (CI) from the queues. Each commit-intent is subject to DSSN validation.
 * The committed transactions will be saved in a backing KV store.
 *
 */

struct Counters {
    uint64_t initialWrites = 0;
    uint64_t rejectedWrites = 0;
    uint64_t precommitReads = 0;
    uint64_t precommitWrites = 0;
    uint64_t commitIntents = 0;
    uint64_t trivialAborts = 0;
    uint64_t busyAborts = 0;
    uint64_t ctsSets = 0;
    uint64_t earlyPeers = 0;
    uint64_t queuedDistributedTxs = 0;
    uint64_t queuedLocalTxs = 0;
    uint64_t precommitReadErrors = 0;
    uint64_t precommitWriteErrors = 0;
    uint64_t preputErrors = 0;
    uint64_t lateScheduleErrors = 0;
    uint64_t readVersionErrors = 0;
    uint64_t concludeErrors = 0;
    uint64_t commitMetaErrors = 0;
    uint64_t commits = 0;
    uint64_t aborts = 0;
    uint64_t commitReads = 0;
    uint64_t commitWrites = 0;
    uint64_t commitOverwrites = 0;
    uint64_t commitDeletes = 0;
};

class Validator {
    PROTECTED:

    //Fixme: rename to KVStore kvStore
    HashmapKVStore &kvStore; //Fixme
    DSSNService *rpcService;
    bool isUnderTest;
	WaitList &localTxQueue;
    SkipList &reorderQueue;
    DistributedTxSet &distributedTxSet;
    ActiveTxSet &activeTxSet;
    PeerInfo &peerInfo;
	ConcludeQueue &concludeQueue;
    ClusterTimeService clock;
    uint64_t lastScheduledTxCTS = 0;
    //LATER DependencyMatrix blockedTxSet;
    Counters counters;

    // threads
    std::thread schedulingThread;
    std::thread serializeThread;
    std::thread peeringThread;

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
    void peer();

    PUBLIC:

	Validator(HashmapKVStore &kvStore, DSSNService *rpcService = NULL, bool isTesting = false);
	~Validator();

    // used by tx RPC handlers
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
     *
     * receive/replySSNInfo handle the peer SSN info exchange.
     */
    bool read(KLayout& k, KVLayout *&kv);
    bool initialWrite(KVLayout &kv);
    bool insertTxEntry(TxEntry *txEntry);
    bool updatePeerInfo(uint64_t cts, uint64_t peerId, uint64_t eta, uint64_t pi, TxEntry *&txEntry);
    bool insertConcludeQueue(TxEntry *txEntry);
    TxEntry* receiveSSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState);
    void replySSNInfo(uint64_t peerId, uint64_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState);
    void sendTxCommitReply(TxEntry *txEntry);

    // used for invoking RPCs
    void sendSSNInfo(TxEntry *txEntry);
    void requestSSNInfo(TxEntry *txEntry, uint64_t targetPeerId);

    // used for obtaining clock value in nanosecond unit
    uint64_t getClockValue();
    uint64_t convertStampToClockValue(uint64_t timestamp);

    // for unit testing, triggering a run of functions without using threads
    bool testRun();
}; // end Validator class

} // end namespace DSSN

#endif  /* VALIDATOR_H */

