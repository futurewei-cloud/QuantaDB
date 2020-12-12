/* Copyright 2020 Futurewei Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef VALIDATOR_H
#define VALIDATOR_H

#include "Common.h"
#include "ActiveTxSet.h"
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
#include "TxLog.h"
#include <stdarg.h>

namespace QDB {

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
    std::atomic<uint64_t> serverId{0};
    std::atomic<uint64_t> initialWrites{0};
    std::atomic<uint64_t> rejectedWrites{0};
    std::atomic<uint64_t> precommitReads{0};
    std::atomic<uint64_t> precommitWrites{0};
    std::atomic<uint64_t> commitIntents{0};
    std::atomic<uint64_t> duplicates{0};
    std::atomic<uint64_t> recovers{0};
    std::atomic<uint64_t> trivialAborts{0};
    std::atomic<uint64_t> busyAborts{0};
    std::atomic<uint64_t> ctsSets{0};
    std::atomic<uint64_t> addPeers{0};
    std::atomic<uint64_t> earlyPeers{0};
    std::atomic<uint64_t> matchEarlyPeers{0};
    std::atomic<uint64_t> latePeers{0};
    std::atomic<uint64_t> deletedPeers{0};
    std::atomic<uint64_t> queuedDistributedTxs{0};
    // scheduledDistributedTxs tracked by distributedTxSet
    // evaluatedDistributedTxs tracked bydistributedT
    // queuedLocalTxs tracked by localTxQueue
    // evaluatedLocalTxs tracked by localTxQueue
    std::atomic<uint64_t> infoSends{0};
    std::atomic<uint64_t> infoReceives{0};
    std::atomic<uint64_t> infoRequests{0};
    std::atomic<uint64_t> infoReplies{0};
    std::atomic<uint64_t> infoLogReplies{0};
    std::atomic<uint64_t> precommitReadErrors{0};
    std::atomic<uint64_t> precommitWriteErrors{0};
    std::atomic<uint64_t> preputErrors{0};
    std::atomic<uint64_t> lateScheduleErrors{0};
    std::atomic<uint64_t> readVersionErrors{0};
    std::atomic<uint64_t> concludeErrors{0};
    std::atomic<uint64_t> alertAborts{0};
    std::atomic<uint64_t> commits{0};
    std::atomic<uint64_t> aborts{0};
    std::atomic<uint64_t> commitReads{0};
    std::atomic<uint64_t> commitWrites{0};
    std::atomic<uint64_t> commitOverwrites{0};
    std::atomic<uint64_t> commitDeletes{0};
};

/*enum LogLevel {
    LOG_NONE = 0u,
    LOG_ERROR = 1u,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
    LOG_ALWAYS = 5u,
};*/

static const uint32_t LOG_BASELINE = 0u;
static const uint32_t LOG_ERROR = 1u;
static const uint32_t LOG_WARN = 2u;
static const uint32_t LOG_INFO = 3u;
static const uint32_t LOG_DEBUG = 4u;
static const uint32_t LOG_ALWAYS = LOG_BASELINE;

class Validator {
    PROTECTED:

    //Fixme: rename to KVStore kvStore
    HashmapKVStore &kvStore; //Fixme
    DSSNService *rpcService;
    bool isUnderTest;
    bool isAlive = true;
	WaitList &localTxQueue;
    SkipList<__uint128_t> &reorderQueue;
    DistributedTxSet &distributedTxSet;
    ActiveTxSet &activeTxSet;
    PeerInfo &peerInfo;
	ConcludeQueue &concludeQueue;
	TxLog &txLog;
    ClusterTimeService clock;
    __uint128_t lastScheduledTxCTS;
    //LATER DependencyMatrix blockedTxSet;
    Counters counters;
    uint32_t logLevel = LOG_INFO;

    // threads
    std::thread schedulingThread;
    std::thread serializeThread;
    std::thread peeringThread;

    // all SSN data maintenance operations
    bool updateKVReadSetPStamp(TxEntry& txEntry);
    bool updateKVWriteSet(TxEntry& txEntry);


    // schedule SSN validation on distributed transactions
    /// move due CIs from reorderQueue into blockedTxSet
    void scheduleDistributedTxs();

    // serialization of commit-intent validation
    void serialize();

    // perform SSN validation on a local transaction
    bool validateLocalTx(TxEntry& txEntry);

    // handle validation commit/abort conclusion
    /// move committed data into backing store and update meta data
    bool conclude(TxEntry *txEntry);

    // handle garbage collection
    void peer();

    // reconstruct meta data from tx log
    bool recover();

    // put counters values into tx log, depending on log level
    bool logCounters();

    // put arbitrary message into tx log, depending on log level
    /// (3,4) is used because there is implicit 'this' parameter in argument list
    bool logMessage(uint32_t level, const char* fmt, ...) __attribute__ ((format (gnu_printf, 3, 4)));


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
    TxEntry* receiveSSNInfo(uint64_t peerId, __uint128_t cts,
            uint64_t pstamp, uint64_t sstamp, uint32_t peerTxState,
            uint64_t &myPStamp, uint64_t &mySStamp, uint32_t &myTxState);
    void replySSNInfo(uint64_t peerId, __uint128_t cts, uint64_t pstamp, uint64_t sstamp, uint8_t peerTxState);
    void sendTxCommitReply(TxEntry *txEntry);

    // calculate sstamp and pstamp using local read/write sets
    bool updateTxPStampSStamp(TxEntry& txEntry);

    // used for invoking RPCs
    void sendSSNInfo(TxEntry *txEntry, bool isSpecific = false, uint64_t targetPeerId = 0);
    void requestSSNInfo(TxEntry *txEntry, bool isSpecific = false, uint64_t targetPeerId = 0);

    // used for obtaining clock value in nanosecond unit
    uint64_t getClockValue();
    __uint128_t get128bClockValue();

    // used for updating counters
    Counters& getCounters() {return counters;}

    // put commit intent into tx log, depending on log level
    bool logTx(uint32_t currentLevel, TxEntry *txEntry);

    // used for setting debug logging level
    void setLogLevel(uint32_t level) {logLevel = (level < LOG_DEBUG) ? level : LOG_DEBUG;}

    // for unit testing, triggering a run of functions without using threads
    bool testRun();
}; // end Validator class

} // end namespace QDB

#endif  /* VALIDATOR_H */

