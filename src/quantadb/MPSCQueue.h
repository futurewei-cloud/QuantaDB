/* Copyright 2021 Futurewei Technologies, Inc.
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

#pragma once
#include <atomic>

namespace QDB {

/**
 * The MPSCQueue class implements a lockfree Multi Producers Single Consumer
 * Queue (MPSCQueue).
 *
 * The MPSCQueue internally maintains two queues.  At any given time, the
 * consumer dequeues the elements off one of the queue (i.e. consumer queue)
 * while the produers are inserting elements into the other queue (i.e.
 * producer queue).   This ensures the comsumer and the producers don't block
 * each other.
 *
 * Once the consumer consumes all of the elements on the current queue.
 * The consumer will swap the queues with the producers.
 *
 * In term of the performance, on the intel xeon E5-2683 v4,
 *   Get(): 200M/sec
 *   Put(): 25M/sec for 1 producer, 5M/sec for > 1 producers
 */
#define MPSCQ_DEFAULT_CAP (64)

template<typename T>
class MPSCQueue {
  /**
   * The internal queue data structure
   */
    struct Queue {
        Queue(uint64_t size) {
	    mElemPtrs = new T[size];
	    mIndex = 0;
	    mRemovedIndex = 0;
	    mCapacity = size;
	    for (uint64_t i = 0; i<size; i++) {
	        mElemPtrs[i] = 0;
	    }
	}
      /**
       * Insert an element into the Queue
       * Return true if there is a space
       */
        inline bool put(T pelem) {
	    uint64_t myIndex = __sync_fetch_and_add(&mIndex, 1);
	    if (myIndex < mCapacity) {
	        mElemPtrs[myIndex] = pelem;
		return true;
	    } else {
	        mIndex = mCapacity;
	    }
	    return false;
	}
      /**
       * Return an element from the queue and advance the index to the next
       * element.
       * Return NULL to the consumer if all of the elements are consumed.
       */
        inline T get() {
	    T pelem = NULL;
	    if (mRemovedIndex < mIndex) {
	        if (!mElemPtrs[mRemovedIndex]) {
		    return NULL;
		}
	        pelem = mElemPtrs[mRemovedIndex];
		mElemPtrs[mRemovedIndex] = NULL;
		mRemovedIndex++;
	    }
	    return pelem;
	}
      /**
       * Helper function to check if all elements have already consumed
       * on the current Queue
       */
        inline bool isEmpty() {
	    if (mRemovedIndex == mIndex) return true;
	    return false;
	}
      /**
       * Helper function to clear the state of the current Queue
       */
        inline void clear() {
	    mIndex = 0;
	    mRemovedIndex = 0;
	}
        T *mElemPtrs;
        uint64_t mIndex;
        uint64_t mRemovedIndex;
        uint64_t mCapacity;
    };
  public:
    /**
     * The constructor for the MPSCQueue.
     * \param size
     *      The capacity of the MPSCQueue
     */
      MPSCQueue(uint64_t size) {
          mQueue[0] = new Queue(size);
	  mQueue[1] = new Queue(size);
	  mPQIndex = 0;
	  mCQIndex = 1;
	  mCapacity = size;
      }
      MPSCQueue(): MPSCQueue(MPSCQ_DEFAULT_CAP) { }
      ~MPSCQueue () {
	  delete mQueue[0];
	  delete mQueue[1];
      }
      /**
       * Insert an element to the current producer queue.  Return true if
       * the element is inserted succesfully.
       * \param pelem
       *      The element to be inserted
       */
      bool put(T pelem) {
	  return mQueue[mPQIndex]->put(pelem);
      }
      /**
       * Retrieve a element from the current consumer queue.  Return Null if
       * both queues are empty.
       * If the current consumer queue is empty, the function attempts to switch
       * to the other queue.
       *
       */
      T get() {
          if (isCQEmpty()) {
              if (!isPQEmpty()) {
                  switchQ();
              }
          }
	  return mQueue[mCQIndex]->get();
      }
      uint32_t getCapacity() { return mCapacity; }
  private:
      /**
       * helper function to check if the current producer queue is empty
       * return true if the queue is empty
       */
      inline bool isPQEmpty() {
	  return mQueue[mPQIndex]->isEmpty();
      }
      /**
       * helper function to check if the current consumer queue is empty
       * return true if the queue is empty
       *
       */
      inline bool isCQEmpty() {
	  return mQueue[mCQIndex]->isEmpty();
      }
      /**
       * helper function to swap the consumer queue with the producer queue
       *
       */
      inline void switchQ() {
	  uint64_t nextPQIndex = (mPQIndex + 1) % 2;
	  mCQIndex = mPQIndex;
	  mQueue[nextPQIndex]->clear();
	  mPQIndex = nextPQIndex;
      }
      std::atomic<uint32_t> mPQIndex;
      uint32_t mCQIndex;
      Queue* mQueue[2];
      uint32_t mCapacity;
 };
}
