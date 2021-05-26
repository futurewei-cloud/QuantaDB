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
#include <functional>
#include "MPSCQueue.h"
#include "Cycles.h"

namespace QDB {

struct Task {
    std::function<void()> callback;
};

 class TaskMemPool {
 public:
     TaskMemPool(uint64_t poolSize=1024) {
         mMemPool = (Task **)calloc(sizeof(Task *), poolSize);
         mTail = 0;
         mPoolSize = poolSize;
     }
     ~TaskMemPool() {
         __sync_synchronize();
         int64_t tail = mTail;
         for (int64_t i = (tail - 1); i >= 0; i--) {
             delete mMemPool[i];
         }
     }
     inline Task *allocTask() {
         Task* t = NULL;
         int64_t idx = mTail;

         if (idx < 1) {
             //Allocate a new memory buffer since we already exhause all
             t = new Task();
         } else {
             //1. get the index to the last memory pointer.
             while (!__sync_bool_compare_and_swap(&mTail, idx, (idx-1))) {
                 idx = mTail;
                 if (idx < 1) {
                     t = new Task();
                     return t;
                 }
             }
             //2. For the memory buffer reused, we need to wait for the memory pointer to be
             //propogated.
             do {
                 t = mMemPool[idx-1];
             } while (!t);
             mMemPool[idx-1] = NULL;
         }
         return t;
     }

     inline void freeTask(Task *t) {
         //1. allocate a memory pointer slot
         int64_t idx = __sync_fetch_and_add(&mTail, 1);
         if (idx < mPoolSize) {
             /*
              *2. insert the memory pointer.  Since index update and the memory pointer slot
              *are not atomic, we need to wait for the memory update
              */
             while (mMemPool[idx-1] != NULL);
             mMemPool[idx-1] = t;
         } else {
             delete t;
         }
     }
 private:
     Task** mMemPool;
     int64_t mTail;
     int64_t mPoolSize;
 };

 class WorkerPool;
 
 class Worker {
     enum WorkerAction {
         WORKER_ACTION_RESET,
         WORKER_GO_IDLE,
         WORKER_GO_ACTIVE,
         WORKER_GO_EXIT
     };
     
 public:
     Worker(uint64_t id, WorkerPool* wp) {
         mAction = WORKER_GO_IDLE;
         mId = id;
         mNumSpinup = 0;
         mNumTasks = 0;
         mMaxSpinupLatencyCycles = 0;
         mWorkerPool = wp;
         auto workerTask = std::bind(&Worker::workerTask, this);
         mThread = new std::thread(workerTask);
     }
     ~Worker() {
         mAction = WORKER_GO_EXIT;
         mCV.notify_one();
         mThread->join();
         delete mThread;
     }
     void goActive() {
         mAction = WORKER_GO_ACTIVE;
         mCV.notify_one();
         mSpinupRequestTimeStamp = RAMCloud::Cycles::rdtsc();
     }
     void goIdle() {
         mAction = WORKER_GO_IDLE;
     }
     void workerTask();

     bool enqueue(Task* t) {
         bool result = false;
         if (mAction == WORKER_GO_ACTIVE) {
             result = mTasks.put(t);
             if (result) {
                 mNumTasks++;
             }
         }
         return result;
     }
     uint64_t getTaskQueueLength() {
         return mNumTasks;
     }
     uint64_t getId() { return mId; }
     uint64_t getSpinupLatencyUs() {
         uint64_t spinupTime = 0;
         uint64_t avgSpinupCycles = 0;
         if (mNumSpinup) {
             avgSpinupCycles = mSpinupLatencyCycles/mNumSpinup;
         }
         spinupTime = RAMCloud::Cycles::toMicroseconds(avgSpinupCycles);
         return spinupTime;
     }
     uint64_t getMaxSpinupLatencyUs() {
         return RAMCloud::Cycles::toMicroseconds(mMaxSpinupLatencyCycles);
     }
 private:
     uint64_t mId;
     std::thread* mThread;
     WorkerPool* mWorkerPool;
     std::mutex mMtx;
     std::condition_variable mCV;
     WorkerAction mAction;
     MPSCQueue<Task *> mTasks;
     std::atomic<uint64_t> mNumTasks;
     /*
      * Counters to track the spinup latency
      */
     uint64_t mSpinupRequestTimeStamp;
     uint64_t mSpinupLatencyCycles;
     uint64_t mNumSpinup;
     uint64_t mMaxSpinupLatencyCycles;
     static const uint64_t mCounterResetCycles = 100;
 };

 class WorkerPool {
 public:
     WorkerPool(uint64_t maxWorkers, uint64_t minWorkers = 1, int highWM=10, int lowWM=2) {
         mNumActiveWorkers = 0;
         mTotalWorkers = maxWorkers;
         mMinActiveWorkers = minWorkers;
         assert (minWorkers <= maxWorkers);
         mHighWaterMark = highWM;
         mLowWaterMark = lowWM;
         mIdleList.reserve(maxWorkers);
         mActiveList.reserve(maxWorkers);
         mRoundRabinIndex = 0;
         mShutdown = false;
         mThreadPinningCountDown = 0;
         mPinnedThreadIdx = 0;
         // 1. Create the Workers & Enqueue the Workers to the idle list
         for (uint64_t i = 0; i < maxWorkers; i++) {
             Worker* w = new Worker(i+1, this);
             mIdleList.push_back(w);
         }
         mNextAdjustCycle = RAMCloud::Cycles::rdtsc() +
             RAMCloud::Cycles::fromMicroseconds(mAdjustmentIntervalUs);
     }

     ~WorkerPool() {
         mShutdown = true;

         while(getNumIdleWorkers()> 0) {
             Worker* worker = mIdleList.back();
             delete worker;
             mIdleList.pop_back();
         }
         while(getNumActiveWorkers()> 0) {
             Worker* worker = mActiveList.back();
             delete worker;
             mActiveList.pop_back();
         }
     }

     uint64_t getNumActiveWorkers() {
         return mNumActiveWorkers;
     }
     uint64_t getNumIdleWorkers() {
         return (mTotalWorkers - mNumActiveWorkers);
     }

     uint64_t isTaskQueuesEmpty() {
         //For unit testing only
         uint64_t count = 0;
         const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);

         for (uint64_t i = 0; i < mActiveList.size(); i++) {
             Worker* w = mActiveList.at(i);
             count += w->getTaskQueueLength();
         }
         for (uint64_t i = 0; i < mIdleList.size(); i++) {
             Worker* w = mIdleList.at(i);
             count += w->getTaskQueueLength();
         }

         return (count == 0);
     }

     uint64_t getAvgSpinupLatencyUs() {
         uint64_t latency = 0;
         uint64_t numWorkers = 0;
         const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);

         for (uint64_t i = 0; i < mActiveList.size(); i++) {
             Worker* w = mActiveList.at(i);
             latency += w->getSpinupLatencyUs();
             numWorkers++;
         }

         return (latency/numWorkers);
     }

     uint64_t getMaxSpinupLatencyUs() {
         uint64_t latency = 0;
         const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);

         for (uint64_t i = 0; i < mActiveList.size(); i++) {
             Worker* w = mActiveList.at(i);
             if (latency < w->getMaxSpinupLatencyUs()) {
                 latency = w->getMaxSpinupLatencyUs();
             }
         }

         return latency;
     }


     /**
      * The enqueue API
      * - The application need to allocate the memory for the task.  The WorkerPool will free
      *   its memory when the task execution is completed
      * - The enqueue function consists of two parts:
      *   1) enqueue the incoming task in the round-rabin fashion among all active workers
      *   2) spin up/down of the number of active workers based on the load, and the load
      *   monitoring+control is done periodically.  The algorithm is described as follow:
      *
      *   When the task queue of the active workers exceed the high watermark,
      *   the WorkerPool library will spin up an active worker from the IdleList.  This active
      *   worker will temporary absorb the incoming loads up to the (highwatermark - 1).
      *   Ideally, we should load balance all existing pending tasks to it.  However,
      *   that will require a slower multi-producer + multi-consumer queue per worker and
      *   load balance will be more expensive since we need to iterate through all of the active
      *   workers.  For spin down, when the active workers' queues are below the low watermark
      *   an active worker will be put to sleep and moved into the idle queue.
      *
      *   To approximate average active workers' queue length, for spinup, we take queue length
      *   of the youngest Active worker.  For spindown, we take the queue length of the oldest
      *   active worker.
      *
      *   We will always have 1 worker active at all time even there is no incoming request to
      *   reduce the latency due to the spin up.
      *
      *   Property: the WorkerPool library bounds the processing latency up to (HighWaterMark *
      *   requestProcessingLatency) for most of the time.
      **/

     void enqueue(Task* task);

     inline void freeTask(Task* t) {
         delete t;
#if 0
         mTaskMemPool.freeTask(t);
#endif
     }
     inline Task* allocTask() {
         Task* t = new Task();
#if 0
         //  Use the Task memory pool
         Task* t = mTaskMemPool.allocTask();
#endif
         return t;
     }
 private:
     std::vector<Worker*> mActiveList;
     std::vector<Worker*> mIdleList;
     uint64_t mTotalWorkers;
     uint64_t mMinActiveWorkers;
     std::atomic<uint64_t> mNumActiveWorkers;
     std::mutex mWorkerMgmtLock;
     uint64_t mRoundRabinIndex;
     uint64_t mThreadPinningCountDown;
     uint64_t mPinnedThreadIdx;
     //Task Memory Pool
     TaskMemPool mTaskMemPool;
     /*
      * Thread pool management controls
      */
     uint64_t mHighWaterMark;
     uint64_t mLowWaterMark;
     uint64_t mNextAdjustCycle;
     // Flag to indicate the worker pool is shutting down
     bool mShutdown;
     static const uint64_t mAdjustmentIntervalUs = 50;
 };
}
