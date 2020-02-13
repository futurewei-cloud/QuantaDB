/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef TX_ENTRY_H
#define TX_ENTRY_H

#include "Common.h"
#include "Object.h"
#include "HOTKV.h"

namespace DSSN {

typedef RAMCloud::Object Object;
typedef RAMCloud::KeyLength KeyLength;

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
    uint64_t cts; //commit time-stamp, globally unique
    uint64_t eta;
    uint64_t pi;
    uint32_t txState;
    uint32_t commitIntentState;
    std::vector<uint64_t> shardSet; //set of participating shards
    std::vector<RAMCloud::Object *> writeSet; //coordinator, for 'is in' operation, may benefit from using 'set' instead?!
    std::vector<RAMCloud::Object *> readSet; //coordinator, for 'remove' operation, may benefit from using 'set' instead?!
    /* Henry: possibly put parameterized Bloom Filters here.
    BloomFilter writeSetFilter;
    BloomFilter readSetFilter;
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

        /* Transaction commit-intent is scheduled, but its local SSN eta and pi could be bogus */
        TX_CI_TRANSIENT = 4,

        /* Transaction commit-intent is scheduled, and its local SSN eta and pi can be used */
        TX_CI_INPROGRESS = 5,

        /* Transaction commit-intent is scheduled, and its local SSN eta and pi are finalized */
        TX_CI_CONCLUDED = 6
    };

    enum {
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

    TxEntry();
    ~TxEntry();
    inline uint64_t getCTS() { return cts; }
    inline uint64_t getEta() { return eta; }
    inline uint64_t getPi() { return pi; }
    inline uint32_t getTxState() { return txState; }
    inline uint32_t getTxCIState() { return commitIntentState; }
    inline std::vector<uint64_t>& getShardSet() { return shardSet; }
    inline std::vector<RAMCloud::Object *>& getWriteSet() { return writeSet; }
    inline std::vector<RAMCloud::Object *>& getReadSet() { return readSet; }
    inline void setCTS(uint64_t val) { cts = val; }
    inline void setPi(uint64_t val) { pi = val; }
    inline void setEta(uint64_t val) { eta = val; }
    inline void setTxState(uint32_t val) { txState = val; }
    inline void setTxCIState(uint32_t val) { commitIntentState = val; }
    inline bool isExclusionViolated() { return pi <= eta; }
    void insertWriteSet(RAMCloud::Object* object) { writeSet.push_back(object); }
    void insertReadSet(RAMCloud::Object* object) { readSet.push_back(object); }

}; // end TXEntry class

} // end namespace DSSN

#endif  /* TX_ENTRY_H */

