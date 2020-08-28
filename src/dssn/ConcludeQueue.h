/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef CONCLUDE_QUEUE_H
#define CONCLUDE_QUEUE_H

#include <boost/lockfree/queue.hpp>
#include "TxEntry.h"

namespace DSSN {

/**
 * This class implements (multi-prodcuer-single-consumer) queueing
 * of validated cross-shard transactions.
 */
class ConcludeQueue {
    PROTECTED:
	boost::lockfree::queue<TxEntry*> inQueue{10000};
	std::atomic<uint32_t> inCount{0};
    std::atomic<uint32_t> outCount{0};

    PUBLIC:
	ConcludeQueue() { ; }

	//for dequeueing by the consumer
    bool try_pop(TxEntry *&txEntry) {
    	if ((inCount - outCount) > 0 && inQueue.pop(txEntry)) {
        	outCount++;
        	return true;
    	}
    	return false;
    }

    //for enqueueing by producers
    bool push(TxEntry *txEntry) {
    	if (inQueue.push(txEntry)) {
    		inCount++;
    		return true;
    	}
    	return false;
    }
}; // end ConcludeQueue class

} // end namespace DSSN

#endif  /* CONCLUDE_QUEUE_H */

