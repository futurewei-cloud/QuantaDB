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
	std::atomic<uint32_t> count{0};

    PUBLIC:
	ConcludeQueue() { count = 0; }

	//for dequeueing by the consumer
    bool try_pop(TxEntry *&txEntry) {
    	if (count > 0 && inQueue.pop(txEntry)) {
        	count--;
        	return true;
    	}
    	return false;
    }

    //for enqueueing by producers
    bool push(TxEntry *txEntry) {
    	if (inQueue.push(txEntry)) {
    		count++;
    		return true;
    	}
    	return false;
    }
}; // end ConcludeQueue class

} // end namespace DSSN

#endif  /* CONCLUDE_QUEUE_H */

