/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef WAIT_QUEUE_H
#define WAIT_QUEUE_H

#include <boost/lockfree/spsc_queue.hpp>
#include <boost/lockfree/queue.hpp>
#include "TxEntry.h"

namespace DSSN {

/**
 * This class implements (multi-prodcuer-single-consumer) queueing
 * of local transaction commit intents.
 * It provides enqueue interface for producers, that include
 * the commit-intent RPC handlers and the serializer as a re-queue-er.
 * It provides dequeue interface for the serializer.
 *
 * The purpose is to optimize serializer performance.
 * TBB concurrent queue and boost::lockfree::queue were tried and had similar performance.
 * For now, we have settled for boost single-producer-single-consumer queue as it has shown
 * 3x performance improvement. Therefore, we use boost spsc queue for interfacing
 * with the consumer, i.e., serializer.
 *
 * The class embeds a queueing thread that inspects a boost (mpmc) queue and the second spsc qeueue.
 * The former is for multiple commit-intent (CI) RPC handlers to enqueue.
 * The latter is for the serializer to re-queue.
 * The thread then transfers CIs from those queues into the first spsc queue.
 *
 */
class WaitQueue {
    PROTECTED:

	boost::lockfree::queue<TxEntry*> inQueue{10000};
	boost::lockfree::spsc_queue<TxEntry*, boost::lockfree::capacity<1000000>> outQueue;
	boost::lockfree::spsc_queue<TxEntry*, boost::lockfree::capacity<10000>> recycleQueue;

	void schedule(bool isUnderTest);

    PUBLIC:
	//for dequeueing by the consumer
    bool try_pop(TxEntry *&txEntry) {
    	return outQueue.pop(txEntry);
    }

    //for enqueueing by producers
    bool push(TxEntry *txEntry) {
    	return inQueue.push(txEntry);
    }

    //for high-performance re-queueing by the consumer
    bool repush(TxEntry *txEntry) {
    	return recycleQueue.push(txEntry);
    }

    //start thread
    void start();
}; // end WaitQueue class

} // end namespace DSSN

#endif  /* ACTIVE_TX_SET_H */

