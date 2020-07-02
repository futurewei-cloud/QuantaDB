
/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 * All rights are reserved.
 */

#ifndef WAITLIST_H
#define WAITLIST_H

#include "TxEntry.h"

namespace DSSN {

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

    PUBLIC:
    std::atomic<uint64_t> addedTxCount{0};
    uint64_t removedTxCount = 0;

    // return true if the CI is added successfully
    bool add(TxEntry *txEntry) {
        assert(txEntry != NULL);
        uint32_t oldTail = tail.load();
        if ((oldTail + 1) % size == head)
            return false; //because there is no room
        if (tail.compare_exchange_strong(oldTail, (oldTail + 1) % size)) {
            assert(txs[oldTail] == NULL);
            txs[oldTail] = txEntry;
            addedTxCount++;
            return true;
        }
        return false; //let caller retry if needed
    }

    // return NULL if iteration stops or fails to find a valid entry
    TxEntry* findFirst(uint64_t &it) {
        it = head;
        while (it != tail) {
            if (txs[it]) {
                return txs[it];
            }
            it = (it + 1) % size;
        }
        return NULL; //no valid entry
    }

    // return NULL if iteration stops or fails to find a valid entry
    TxEntry* findNext(uint64_t &it) {
        while (it != tail) {
            if (txs[it]) {
                return txs[it];
            }
            it = (it + 1) % size;
        }
        return NULL; //no valid entry
    }

    // return true if the CI is removed successfully
    bool remove(uint64_t &it) {
        assert(txs[it] != NULL);
        assert(it != tail);
        txs[it] = NULL;
        while (head != tail) {
            if (txs[head] != NULL)
                break;
            head = (head + 1) % size; //Fixme?
        }
        removedTxCount++;
        return true;
    }

    bool pop(TxEntry *&txEntry) {
        uint64_t it;
        if ((txEntry = findFirst(it)))
            return remove(it);
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
        std::memset(txs, 0, size);
    }

    WaitList(uint32_t _size) {
        size = _size;
        txs = new TxEntry*[size];
        std::memset(txs, 0, size);
    }

    ~WaitList() {
        delete txs;
    }
}; // end WaitList class

} // end namespace DSSN

#endif  // WAITLIST_H
