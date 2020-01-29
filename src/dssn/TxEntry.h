/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef TX_ENTRY_H
#define TX_ENTRY_H

#include "Common.h"
#include "Object.h"

namespace DSSN {

/**
 * Each TxEntry object represents a single transaction attempt to an DSSN validator.
 */
class TxEntry {
  PROTECTED:
    uint64_t txId;
    uint64_t cts; //commit time-stamp
    uint64_t eta;
    uint64_t pi;
    uint32_t txState;
    uint32_t commitIntentState;
    std::vector<uint64_t> shardSet; //set of participating shards
    std::vector<RAMCloud::Object *> writeSet;
    std::vector<RAMCloud::Object *> readSet;

    bool updateEtaPi();
    bool isExclusionViolated();

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



    bool validate();

    inline std::vector<uint64_t>& getShardSet() { return shardSet; }

}; // end TxEntry

} // end namespace DSSN

#endif  /* TX_ENTRY_H */

