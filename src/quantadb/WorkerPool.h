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

#define WORKERPOOL_ENQUEUE_TASK(threadpool, method, object, data)   \
    {                                                               \
        Task* t = threadpool->allocTask();                          \
        t->callback = std::bind(&method, object, data);             \
        threadpool->enqueue(t);                                     \
    } while(0);

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
     enum WorkerState {
         WORKER_IDLE_PENDING,
         WORKER_IDLE,
         WORKER_WARMUP,
         WORKER_ACTIVE,
         WORKER_EXIT
     };
     
 public:
     Worker(uint64_t id, WorkerPool* wp) {
         mState = WORKER_IDLE_PENDING;
         mId = id;
         mNumSpinup = 0;
         mNumTasks = 0;
         mMaxSpinupLatencyCycles = 0;
         mNumTaskExec = 0;
         mTaskExecLatencyCycles = 0;
         mWorkerPool = wp;
         auto workerTask = std::bind(&Worker::workerTask, this);
         mThread = new std::thread(workerTask);
     }
     ~Worker() {
         mState = WORKER_EXIT;
         mCV.notify_one();
         mThread->join();
         delete mThread;
     }
     bool goActive() {
         if (!__sync_bool_compare_and_swap(&mState, WORKER_IDLE, WORKER_WARMUP)) return false;

         std::unique_lock<std::mutex> lock(mMtx);
         mCV.notify_one();
         lock.unlock();
         mSpinupRequestTimeStamp = RAMCloud::Cycles::rdtsc();
         return true;
     }

     bool goIdle() {
         if (!__sync_bool_compare_and_swap(&mState, WORKER_ACTIVE, WORKER_IDLE_PENDING)) return false;
         return true;
     }

     void workerTask();

     inline bool enqueue(Task* t) {
         bool result = false;
         mNumTasks++;
         if (mState == WORKER_ACTIVE) {
             result = mTasks.put(t);
         }
         if (!result) {
             mNumTasks--;
         }
         return result;
     }
     uint64_t getTaskQueueLength() {
         return mNumTasks;
     }
     uint64_t getId() { return mId; }
     uint64_t getNumSpinup() { return mNumSpinup; }
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
     uint64_t getNumTaskExec() {
         return mNumTaskExec;
     }
     uint64_t getTaskExecLatencyUs() {
         uint64_t avgTaskExecCycles = 0;
         if (mNumTaskExec) {
             avgTaskExecCycles = mTaskExecLatencyCycles/mNumTaskExec;
         }
         return RAMCloud::Cycles::toMicroseconds(avgTaskExecCycles);
     }
 private:
     std::atomic<uint64_t> mNumTasks;
     WorkerState mState;
     uint64_t mId;
     std::thread* mThread;
     WorkerPool* mWorkerPool;
     std::mutex mMtx;
     std::condition_variable mCV;
     MPSCQueue<Task *> mTasks;
     /*
      * Counters to track the spinup latency
      */
     uint64_t mSpinupRequestTimeStamp;
     uint64_t mSpinupLatencyCycles;
     uint64_t mNumSpinup;
     uint64_t mMaxSpinupLatencyCycles;
     uint64_t mTaskExecLatencyCycles;
     uint64_t mNumTaskExec;
     static const uint64_t mCounterResetCycles = 100;
 };

 class WorkerPool {
 public:
     WorkerPool(uint64_t maxWorkers, uint64_t minWorkers = 1, double highWM=10, double lowWM=1) {
         mNumActiveWorkers = 0;
         mTotalWorkers = maxWorkers;
         mMinActiveWorkers = (minWorkers > 0) ? minWorkers : 1;
         assert (minWorkers <= maxWorkers);
         mHighWaterMark = highWM;
         mLowWaterMark = (lowWM > 0) ? lowWM : 0;

         mIdleList.reserve(maxWorkers);
         mActiveList.reserve(maxWorkers);
         mRoundRabinIndex = 0;
         mShutdown = false;

         // 1. Create the Workers & Enqueue the Workers to the idle list
         for (uint64_t i = 0; i < maxWorkers; i++) {
             Worker* w = new Worker(i+1, this);
             mIdleList.push_back(w);
             mWorkerList.push_back(w);
         }
         mNextAdjustCycle = 0;
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

         for (uint64_t i = 0; i < mWorkerList.size(); i++) {
             Worker* w = mWorkerList.at(i);
             count += w->getTaskQueueLength();
         }

         return (count == 0);
     }

     double getAvgTaskQueuesLength() {
         uint64_t count = 0;
         double result = 0;
         uint64_t numActiveWorkers = mNumActiveWorkers;

         if (mNumActiveWorkers == 0) return 0;

         for (uint64_t i = (mTotalWorkers - numActiveWorkers); i < mTotalWorkers; i++) {
             Worker* w = mWorkerList.at(i);
             count += w->getTaskQueueLength();
         }

         result = ((double)count)/numActiveWorkers;

         return result;
     }

     uint64_t getAvgSpinupLatencyUs() {
         uint64_t latency = 0;
         uint64_t numWorkers = 0;

         for (uint64_t i = 0; i < mWorkerList.size(); i++) {
             Worker* w = mWorkerList.at(i);
             if (w->getNumSpinup()) {
                 latency += w->getSpinupLatencyUs();
                 numWorkers++;
             }
         }

         return (latency/numWorkers);
     }

     uint64_t getMaxSpinupLatencyUs() {
         uint64_t latency = 0;

         for (uint64_t i = 0; i < mWorkerList.size(); i++) {
             Worker* w = mWorkerList.at(i);
             if (latency < w->getMaxSpinupLatencyUs()) {
                 latency = w->getMaxSpinupLatencyUs();
             }
         }

         return latency;
     }

     uint64_t getAvgTaskExecLatencyUs() {
         uint64_t latency = 0;
         uint64_t numWorkers = 0;

         for (uint64_t i = 0; i < mWorkerList.size(); i++) {
             Worker* w = mWorkerList.at(i);
             if (w->getNumTaskExec()) {
                 latency += w->getTaskExecLatencyUs();
                 numWorkers++;
             }
         }
         if (!numWorkers) return 0;

         return (latency/numWorkers);
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
      *   the WorkerPool library will spin up an active worker from the IdleList.
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
     void resourceManagementTask();

     inline void freeTask(Task* t) {
#ifdef TASKMEMPOOL
         mTaskMemPool.freeTask(t);
#else
         delete t;
#endif
     }
     inline Task* allocTask() {
#ifdef TASKMEMPOOL
         //  Use the Task memory pool
         Task* t = mTaskMemPool.allocTask();
#else
         Task* t = new Task();
#endif
         return t;
     }
 private:
     std::vector<Worker*> mActiveList;
     std::vector<Worker*> mIdleList;
     std::vector<Worker*> mWorkerList;  //Entire worker list (read-only)
     uint64_t mTotalWorkers;
     uint64_t mMinActiveWorkers;
     std::atomic<uint64_t> mNumActiveWorkers;
     uint64_t mRoundRabinIndex;
#ifdef TASKMEMPOOL
     //Task Memory Pool
     TaskMemPool mTaskMemPool;
#endif
     /*
      * Thread pool management controls
      */
     double mHighWaterMark;
     double mLowWaterMark;
     uint64_t mNextAdjustCycle;
     // Flag to indicate the worker pool is shutting down
     bool mShutdown;
     static const uint64_t mAdjustmentIntervalUs = 1000;
 };
}
