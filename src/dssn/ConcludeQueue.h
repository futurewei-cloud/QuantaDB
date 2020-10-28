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
    // Fixme: This section is sometimes observed with loss of event in the queue -- however,
    // we should not trust the counters.
    // Later, experiment getting rid of the mutex.

    PROTECTED:
    boost::lockfree::queue<TxEntry*> inQueue{10000};
    std::mutex mutex;

    PUBLIC:
    std::atomic<uint64_t> inCount{0};
    std::atomic<uint64_t> outCount{0};

    ConcludeQueue() { ; }

    //for dequeueing by the consumer
    bool try_pop(TxEntry *&txEntry) {
        mutex.lock();
        if ((inCount - outCount) > 0 && inQueue.pop(txEntry)) {
            outCount++;
            mutex.unlock();
            return true;
        }
        mutex.unlock();
        return false;
    }

    //for enqueueing by producers
    bool push(TxEntry *txEntry) {
        mutex.lock();
        if (inQueue.push(txEntry)) {
            inCount++;
            mutex.unlock();
            return true;
        }
        mutex.unlock();
        return false;
    }
}; // end ConcludeQueue class

} // end namespace DSSN

#endif  /* CONCLUDE_QUEUE_H */

