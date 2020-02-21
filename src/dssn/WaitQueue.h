/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef WAIT_QUEUE_H
#define WAIT_QUEUE_H

#include <boost/lockfree/spsc_queue.hpp>
#include <atomic>
#include "TxEntry.h"

namespace DSSN {

/**
 * This class implements (multi-prodcuer-single-consumer) queueing
 * of local transaction commit intents.
 * It provides enqueue interface for producers, that include
 * the commit-intent RPC handlers and the serializer as a re-queue-er.
 * It provides dequeue interface for the serializer.
 *
 * We want to optimize serializer performance.
 * TBB concurrent queue and boost::lockfree::queue were tried and had similar performance.
 * For now, we have settled for boost single-producer-single-consumer queue as it has shown
 * 3x performance improvement. Therefore, we use boost spsc queue for interfacing
 * with the consumer, i.e., serializer.
 *
 * The class embeds a queueing thread that inspects a boost (mpmc) queue and the second spsc qeueue.
 * The former is for multiple commit-intent (CI) RPC handlers to enqueue.
 * The latter is for the serializer to re-queue.
 * The thread then transfers CIs from those queues into the first spsc queue
 *
 */
class WaitQueue {
	PRIVATE:
	boost::lockfree::spsc_queue<TxEntry*, boost::lockfree::capacity<1000000>> queue;

    PUBLIC:
    // false if the key is failed to be added due to overflow
    bool try_pop(TxEntry *&txEntry) {
    	return queue.pop(txEntry);
    }

    // for performance, the key is assumed to have been added
    bool push(TxEntry *txEntry) {
    	return queue.push(txEntry);
    }

}; // end WaitQueue class

} // end namespace DSSN

#endif  /* ACTIVE_TX_SET_H */

