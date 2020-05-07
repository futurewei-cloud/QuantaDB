/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef TX_ENTRY_H
#define TX_ENTRY_H

#include "Common.h"
#include "KVStore.h"
#include <mutex>

namespace DSSN {

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
    uint64_t cts; //commit time-stamp, globally unique
    uint64_t pstamp;
    uint64_t sstamp;

    //DSSN tx states
    uint32_t txState;
    uint32_t commitIntentState;

    //mutex for protecting concurrent peer info updates
    ///ideally this mutex should be encapsulated inside PeerInfo class as it is expected
    ///to be used by peer info exchange RPC handlers only; yet, we'd like to avoid
    ///frequent malloc/dealloc of this small object.
    std::mutex mutexForPeerUpdate;

    //RPC handle for replying to commit intent
    void *rpcHandle;

    //Set of IDs of participant shards excluding self
    ///use std::set for sake of equality check
    std::set<uint64_t> peerSet;
    std::set<uint64_t> peerSeenSet;

    //write set and read set under validation
    uint32_t writeSetSize;
    uint32_t readSetSize;
    boost::scoped_array<KVLayout *> writeSet;
    boost::scoped_array<KVLayout *> readSet;

    //Handy hash value of write/read key for Bloom Filter etc.
    //a 64-bit number is composed of 2 32-bit numbers in upper 32 bits and lower 32 bits
    boost::scoped_array<uint64_t> writeSetHash;
    boost::scoped_array<uint64_t> readSetHash;

    //Handy pointer to KV store tuple that is matching the readSet/writeSet key
    boost::scoped_array<KVLayout *> writeSetInStore;
    boost::scoped_array<KVLayout *> readSetInStore;

    //Handy index to resume active tx filter check
    uint32_t writeSetIndex;
    uint32_t readSetIndex;

    /*
    Henry: possibly needs these
    uint64_t id; //transaction globally unique ID
    uint64_t readSnapshotTimestamp; //0 - not a read-only tx, max - most recent, [1, max) - specific snapshot
     */

    PUBLIC:
    enum {
        /* Transaction commit-intent is not or no longer queued for scheduling */
        TX_CI_UNQUEUED = 1,

        /* Transaction commit-intent is queued for scheduling */
        TX_CI_QUEUED = 2,

        /* Transaction commit-intent is blocked from being scheduled due to dependency */
        TX_CI_WAITING = 3,

        /* Transaction commit-intent is scheduled, but its local SSN pstamp and sstamp could be bogus */
        TX_CI_TRANSIENT = 4,

        /* Transaction commit-intent is scheduled, and its local SSN pstamp and sstamp can be used */
        TX_CI_INPROGRESS = 5,

        /* Transaction commit-intent is scheduled, and its local SSN pstamp and sstamp are finalized */
        TX_CI_CONCLUDED = 6,

		/* Transaction commit-intent is finished, and txEntry can be purged */
		TX_CI_FINISHED = 7,
    };

    enum {
        /* Transaction state not yet initialized */
        TX_INVAL = 0,

        /* Transaction is active and in an unstable state. */
        TX_PENDING = 1,

        /* Transaction is aborted. */
        TX_ABORT = 2,

        /* Transaction is validated and committed. */
        TX_COMMIT = 3,

        /* Transaction is deactivated and in an unstable state. The responder will
         * no longer send out its SSN data again.
         */
        TX_ALERT = 4,

        /* Transaction has inconsistent commit and abort decisions among the peers.
         * It is supposed to expose software bugs and require manual recovery because
         * no new transactions involving its read/write sets can proceed.
         */
        TX_CONFLICT = 5
    };

    TxEntry(uint32_t readSetSize, uint32_t writeSetSize);
    ~TxEntry();
    inline uint64_t getCTS() { return cts; }
    inline uint64_t getPStamp() { return pstamp; }
    inline uint64_t getSStamp() { return sstamp; }
    inline uint32_t getTxState() { return txState; }
    inline uint32_t getTxCIState() { return commitIntentState; }
    inline std::mutex& getMutex() { return mutexForPeerUpdate; }
    inline void* getRpcHandle() { return rpcHandle; }
    inline void insertPeerSet(uint64_t peerId) { peerSet.insert(peerId); }
    inline auto& getPeerSet() { return peerSet; }
    inline void insertPeerSeenSet(uint64_t peerId) { peerSeenSet.insert(peerId); }
    inline auto& getPeerSeenSet() { return peerSeenSet; }
    inline bool isAllPeerSeen() { return peerSeenSet == peerSet; }
    inline auto& getWriteSet() { return writeSet; }
    inline auto& getReadSet() { return readSet; }
    inline auto getWriteSetSize() { return writeSetSize; }
    inline auto getReadSetSize() { return readSetSize; }
    inline auto& getWriteSetHash() { return writeSetHash; }
    inline auto& getReadSetHash() { return readSetHash; }
    inline auto& getWriteSetInStore() { return writeSetInStore; }
    inline auto& getReadSetInStore() { return readSetInStore; }
    inline auto& getWriteSetIndex() { return writeSetIndex; }
    inline auto& getReadSetIndex() { return readSetIndex; }
    inline void setCTS(uint64_t val) { cts = val; }
    inline void setSStamp(uint64_t val) { sstamp = val; }
    inline void setPStamp(uint64_t val) { pstamp = val; }
    inline void setTxState(uint32_t val) { txState = val; }
    inline void setTxCIState(uint32_t val) { commitIntentState = val; }
    inline void setRpcHandle(void *rpc) { rpcHandle = rpc; }
    inline bool isExclusionViolated() { return sstamp <= pstamp; }
    bool insertWriteSet(KVLayout* kv, uint32_t i);
    bool insertReadSet(KVLayout* kv, uint32_t i);
    inline void insertWriteSetInStore(KVLayout* kv, uint32_t i) { writeSetInStore[i] = kv; }
    inline void insertReadSetInStore(KVLayout* kv, uint32_t i) { readSetInStore[i] = kv; }
    uint32_t serializeSize();
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

} // end namespace DSSN

#endif  /* TX_ENTRY_H */

