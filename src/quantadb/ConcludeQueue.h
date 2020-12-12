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

#ifndef CONCLUDE_QUEUE_H
#define CONCLUDE_QUEUE_H

#include <boost/lockfree/queue.hpp>
#include "TxEntry.h"

namespace QDB {

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

} // end namespace QDB

#endif  /* CONCLUDE_QUEUE_H */

