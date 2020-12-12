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

#ifndef WAITLIST_H
#define WAITLIST_H

#include "TxEntry.h"

namespace QDB {

/*
 * The class is purpose-built for holding commit intents (CIs) before moving
 * them into activeTxSet.
 *
 * The add() is thread-safe, expecting multiple producers.
 * The findFirst(), findNext(), and remove() are to be used by a single consumer,
 *
 * It is implemented as a circular array tracked by head and tail.
 */
class WaitList {
    PROTECTED:

    uint32_t size = 65536;

    //array of commit-intents, NULL meaning empty slot
    TxEntry* *txs;

    //indexes of the above arrays
    std::atomic<uint32_t> head{0};
    std::atomic<uint32_t> tail{0};

    //for performance optimization
    uint64_t activitySignature = -1;

    std::mutex mutexForAdd;

    PUBLIC:
    std::atomic<uint64_t> addedTxCount{0};
    std::atomic<uint64_t> removedTxCount{0};

    // return true if the CI is added successfully
    bool add(TxEntry *txEntry) {
        /*
         * The use of mutex is safeguard a race condition that multiple
         * callers have advanced the tail but are populating txs[] at different
         * time out of order, giving a chance for the remove() to advance the
         * head beyond a txs[] that would have been populated later.
         */
        assert(txEntry != NULL);
        mutexForAdd.lock();
        uint32_t oldTail = tail.load();
        if ((oldTail + 1) % size == head) {
            mutexForAdd.unlock();
            return false; //because there is no room
        }
        if (tail.compare_exchange_strong(oldTail, (oldTail + 1) % size)) {
            assert(txs[oldTail] == 0);
            txs[oldTail] = txEntry;
            addedTxCount.fetch_add(1);
            mutexForAdd.unlock();
            return true;
        }
        mutexForAdd.unlock();
        return false; //let caller retry if needed
    }

    // return NULL if iteration stops or fails to find a valid entry
    TxEntry* findFirst(uint64_t &it) {
        it = head;
        while (it != tail) {
            if (txs[it] != NULL) {
                return txs[it];
            }
            it = (it + 1) % size;
        }
        return NULL; //no valid entry
    }

    // return NULL if iteration stops or fails to find a valid entry
    TxEntry* findNext(uint64_t &it) {
        it = (it + 1) % size;
        while (it != tail) {
            if (txs[it] != NULL) {
                return txs[it];
            }
            it = (it + 1) % size;
        }
        return NULL; //no valid entry
    }

    // return true if the CI is removed successfully
    bool remove(uint64_t &it, const TxEntry *target) {
        assert(txs[it] == target);
        assert(txs[it] != NULL);
        txs[it] = NULL;
        removedTxCount.fetch_add(1);
        while (txs[head] == NULL) {
            uint32_t oldHead = head;
            if (oldHead == (size - 1))
                head = 0;
            else
                head++;
            if (oldHead == it)
                break;
        }
        return true;
    }

    bool pop(TxEntry *&txEntry) {
        uint64_t it;
        if ((txEntry = findFirst(it)))
            return remove(it, txEntry);
        return false;
    }

    // for optimization, as a test for useless iteration
    bool hasNoActivity() {
        if (activitySignature == (addedTxCount + removedTxCount))
            return true;
        activitySignature = (addedTxCount + removedTxCount);
        return false;
    }

    bool isFull() { return head == (tail + 1) % size; }

    // for debugging only - mind the wrap-around
    uint32_t count() { return (addedTxCount - removedTxCount); }
    bool isSane() { return !(addedTxCount != removedTxCount && head == tail); }

    WaitList() {
        txs = new TxEntry*[size];
        std::memset(txs, 0, size * sizeof(TxEntry*));
    }

    WaitList(uint32_t _size) {
        size = _size;
        txs = new TxEntry*[size];
        std::memset(txs, 0, size * sizeof(TxEntry*));
    }

    ~WaitList() {
        delete txs;
    }
}; // end WaitList class

} // end namespace QDB

#endif  // WAITLIST_H
