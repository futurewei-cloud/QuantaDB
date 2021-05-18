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

#ifndef TX_ENTRY_H
#define TX_ENTRY_H

#include "Common.h"
#include "KVStore.h"
#include <mutex>

namespace QDB {

#define TUPLE_ENTRY_MAX 128

/**
 * Each TxEntry object represents a single transaction.
 *
 * The class is expected to be used by the validator and by the coordinator.
 *
 * At the coordinator, its object contains the complete read set and write set of
 * all relevant shards though the sets may be built incrementally. It would use
 * the write set Bloom Filter to facilitate making the read set non-overlapping with
 * the write set. It would also use the write set Bloom Filter to help provide the read operation
 * of any tuple that it has written.
 *
 * At the validator, its object contains the read set and write set of the local shard.
 * It uses the read set Bloom Filter and write set Bloom Filter to facilitate
 * dependency checking for serialization.
 */
class TxEntry {
    PROTECTED:

    //DSSN data
    __uint128_t cts; //commit time-stamp, globally unique, upper 64b with ns precision
    uint64_t pstamp; //with ns precision
    uint64_t sstamp; //with ns precision

    //QDB tx states
    volatile uint32_t txState;
    volatile uint32_t commitIntentState;
    volatile uint32_t commitResult;     // commit result

    //RPC handle for replying to commit intent
    void *rpcHandle;

    //Set of IDs of participant shards excluding self
    ///use std::set for sake of equality check
    std::set<uint64_t> peerSet;
    uint8_t myPeerPosition = 0;

    //write set and read set under validation
    uint32_t writeSetSize;
    uint32_t readSetSize;
    boost::scoped_array<KVLayout *> writeSet;
    boost::scoped_array<KVLayout *> readSet;
    /*
     * Track the Tuple lock state.  If the lockTableFilter lookup performance
     * is ok, we can elminate the write/readSetLockState array.  When we
     * validate the transaction, we can simply iterate through the
     * lockTableFilter to acquire the tuple locks.
     */
    bool writeTupleSkipLock[TUPLE_ENTRY_MAX];
    bool readTupleSkipLock[TUPLE_ENTRY_MAX];
    std::set<uint64_t> lockTableFilter;

    //Cache the KVStore address, where the KV pointer is stored
    boost::scoped_array<void *> writeSetKVSPtr;
    boost::scoped_array<void *> readSetKVSPtr;

    //TODO: remove KVLayout pointers cache
    //Handy pointer to KV store tuple that is matching the readSet/writeSet key
    boost::scoped_array<KVLayout *> writeSetInStore;
    boost::scoped_array<KVLayout *> readSetInStore;

    //Handy index to resume active tx filter check
    uint32_t writeSetIndex;
    uint32_t readSetIndex;

    //Handy hash value of write/read key for Bloom Filter etc.
    //a 64-bit number is composed of 2 32-bit numbers in upper 32 bits and lower 32 bits
    boost::scoped_array<uint64_t> writeSetHash;
    boost::scoped_array<uint64_t> readSetHash;
    PUBLIC:
    bool isOutOfOrder = false; //Fixme: can overload TxCIState later
    // local timer to track performance 
    uint64_t local_commit = 0; 
    enum {
    	//TX_CI_xxx states are for validator internal use to track the progress
    	//through the processing stages. The sequential order must be maintained.

        /// Transaction commit-intent is not queued for scheduling
        TX_CI_UNQUEUED = 1,

        /// Transaction commit-intent is queued for scheduling
        TX_CI_QUEUED,

        /// Transaction commit-intent is scheduled
        TX_CI_SCHEDULED,

        /// Transaction commit-intent has had SSN info sent to peers
        TX_CI_LISTENING,

        /// Transaction commit-intent has reached a decision but not been logged
        TX_CI_CONCLUDED,

        /// Transaction commit-intent has reached a decision and that is logged
        TX_CI_SEALED,

		/// Transaction commit-intent has finished its life, and txEntry can be purged
		TX_CI_FINISHED,
    };

    enum {
    	//TX_xxx states track tx state visible to outside components like peers and coordinator

        /// Transaction is active and in an unstable state
        TX_PENDING = 1,

        /// Transaction has reached an abort conclusion, a stable state
        TX_ABORT = 2,

        /// Transaction has reached a commit conclusion, a stable state
        TX_COMMIT = 3,

		/// Transaction is to be aborted if peers agree, an unstable state
        TX_ALERT = 4,

        /// Transaction has inconsistent commit and abort decisions among the peers.
        /// It is supposed to expose software bugs and require manual recovery because
        /// no new transactions involving its read/write sets can/should proceed.
        TX_CONFLICT = 5,

       /// Indicator of a fabricated tx entry for logging purpose
        TX_FABRICATED = 99
    };

     enum {
	TX_UNCOMMIT = 10,       // Commit: all in from INIT.
	TX_COMMIT_INIT = 11,    // Commit: all in from INIT.
	TX_COMMIT_PEER = 12,    // Commit: all in from INIT.
	TX_ABORT_ALERT = 13,    // Abort from consensused ALERT.
	TX_ABORT_PEER = 14,     // Abort from peer abort.
	TX_ABORT_PISI = 15,     // Abort from Si-Pi conflict.
	TX_ABORT_PISI_INIT = 16, // Abort from initial Si-Pi conflict
	TX_ABORT_LATE = 17, // Abort from initial Si-Pi conflict
	TX_ABORT_TRIVIAL = 18,  // Abort for trivial reason.
     };

    TxEntry(uint32_t readSetSize, uint32_t writeSetSize);
    ~TxEntry();
    inline __uint128_t getCTS() { return cts; }
    inline uint64_t getPStamp() { return pstamp; }
    inline uint64_t getSStamp() { return sstamp; }
    inline uint32_t getTxState() { return txState; }
    inline uint32_t getTxCIState() { return commitIntentState; }
    inline uint8_t getPeerPosition() { return myPeerPosition; }
    inline void* getRpcHandle() { return rpcHandle; }
    inline void insertPeerSet(uint64_t peerId) { peerSet.insert(peerId); }
    inline  std::set< uint64_t>& getParticipantSet() { return peerSet; }
    inline uint64_t getPeerSet() { return ((uint64_t)(1 << (peerSet.size() + 1)) - 1 ) ^ (1 << myPeerPosition); }
    inline boost::scoped_array<KVLayout *>& getWriteSet() { return writeSet; }
    inline boost::scoped_array<KVLayout *>& getReadSet() { return readSet; }
    inline uint32_t getWriteSetSize() { return writeSetSize; }
    inline uint32_t getReadSetSize() { return readSetSize; }
    inline boost::scoped_array<uint64_t>& getWriteSetHash() { return writeSetHash; }
    inline boost::scoped_array<uint64_t>& getReadSetHash() { return readSetHash; }
    inline boost::scoped_array<KVLayout *>& getWriteSetInStore() { return writeSetInStore; }
    inline boost::scoped_array<KVLayout *>& getReadSetInStore() { return readSetInStore; }
    inline void *getReadSetKVSPtr(uint64_t i) { return readSetKVSPtr[i]; }
    inline void *getWriteSetKVSPtr(uint64_t i) { return writeSetKVSPtr[i]; }
    inline bool isReadTupleSkipLock(uint64_t i) { return readTupleSkipLock[i]; }
    inline bool isWriteTupleSkipLock(uint64_t i) { return writeTupleSkipLock[i]; }
    inline uint32_t& getWriteSetIndex() { return writeSetIndex; }
    inline uint32_t& getReadSetIndex() { return readSetIndex; }
    inline void setCTS(__uint128_t val) { cts = val; }
    inline void setSStamp(uint64_t val) { sstamp = val; }
    inline void setPStamp(uint64_t val) { pstamp = val; }
    inline void setTxState(uint32_t val) { txState = val; }
    inline void setTxCIState(uint32_t val) { commitIntentState = val; }
    inline void setPeerPosition(uint32_t val) { myPeerPosition = val; }
    inline void setTxResult(uint32_t val) { commitResult = val; }
    inline void setRpcHandle(void *rpc) { rpcHandle = rpc; }
    inline bool isExclusionViolated() { return sstamp <= pstamp; }
    bool insertWriteSet(KVLayout* kv, uint32_t i);
    bool insertReadSet(KVLayout* kv, uint32_t i);
    inline void insertWriteSetInStore(KVLayout* kv, uint32_t i) { writeSetInStore[i] = kv; }
    inline void insertReadSetInStore(KVLayout* kv, uint32_t i) { readSetInStore[i] = kv; }
    inline void cacheWriteSetKVPtr(void* ptr, uint32_t i) { writeSetKVSPtr[i] = ptr; }
    inline void cacheReadSetKVPtr(void* ptr, uint32_t i) { readSetKVSPtr[i] = ptr; }
    bool correctReadSet(uint32_t size);
 

    const char *getTxResult() {
	switch (commitResult) {
		case TX_UNCOMMIT:
			return ("TX_UNCOMMIT");
		case TX_COMMIT_INIT:
			return ("TX_COMMIT_INIT");
		case TX_COMMIT_PEER:
			return ("TX_COMMIT_PEER");
		case TX_ABORT_ALERT:
			return ("TX_ABORT_ALERT");
		case TX_ABORT_PEER:
			return ("TX_ABORT_PEER");
		case TX_ABORT_PISI:
			return ("TX_ABORT_PISI");
		case TX_ABORT_PISI_INIT:
			return ("TX_ABORT_PISI_INIT");
		case TX_ABORT_LATE:
			return ("TX_ABORT_CONFLICT");
		case TX_ABORT_TRIVIAL:
			return ("TX_ABORT_TRIVIAL");
		default:
			static char output[20];
			sprintf(output, "Failure: %d", commitResult);
			return (output);
	}
    }

    //Fixme: move these functions into TxLog.cc to hide implementation details from TxEntry
    uint32_t serializeSize(uint32_t *ws = NULL, uint32_t *rs = NULL, uint32_t *ps = NULL);
    void serialize( outMemStream& out );
    void deSerialize_common( inMemStream& in );
    void deSerialize_additional( inMemStream& in );
    void deSerialize( inMemStream& in );
}; // end TXEntry class

class TxComparator {
	bool operator()(TxEntry *firstTx, TxEntry *secondTx) const {
		return firstTx->getCTS() < secondTx->getCTS();
	}
};

} // end namespace QDB

#endif  /* TX_ENTRY_H */

