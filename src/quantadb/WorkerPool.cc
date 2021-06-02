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

#include "WorkerPool.h"
#include "Logger.h"
#include "RamCloud.h"

namespace QDB {

void
Worker::workerTask()
{
    while (1) {
        if (mNumTasks > 0) {
            Task* task = mTasks.get();
            if (task != NULL) {
                RAMCLOUD_LOG(NOTICE, "worker %lu start task", mId);
#ifdef PROFILE_TASK_EXEC_TIME
                uint64_t start = RAMCloud::Cycles::rdtsc();
                task->callback();
                mTaskExecLatencyCycles += (RAMCloud::Cycles::rdtsc() - start);
#else
                task->callback();
#endif
                mNumTaskExec++;
                mWorkerPool->freeTask(task);
                mNumTasks--;
                //RAMCLOUD_LOG(ERROR, "worker %lu finish task", mId);
            }
        } else if(mState == WORKER_IDLE_PENDING) {
            if (!__sync_bool_compare_and_swap(&mState, WORKER_IDLE_PENDING, WORKER_IDLE) || mNumTasks) continue;
            std::unique_lock<std::mutex> lock(mMtx);
            mCV.wait(lock);
            lock.unlock();
#ifdef PROFILE_TASK_EXEC_TIME
            // Track the spinup latency statistic
            mNumSpinup++;
            uint64_t latencyCycles = RAMCloud::Cycles::rdtsc() - mSpinupRequestTimeStamp;
            mSpinupLatencyCycles += latencyCycles;
            if (latencyCycles > mMaxSpinupLatencyCycles) {
                mMaxSpinupLatencyCycles = latencyCycles;
            }
            if (mNumSpinup == mCounterResetCycles) {
                mSpinupLatencyCycles = 0;
                mNumSpinup = 0;
                mTaskExecLatencyCycles = 0;
                mNumTaskExec = 0;
            }
#endif
        } else if (mState == WORKER_WARMUP) {
            mState = WORKER_ACTIVE;
        } else if ((mState == WORKER_EXIT)) {
            return;
        }
    }
}

void
WorkerPool::enqueue(Task* task) {
    if (mShutdown) return;
    /* 1. choose an active worker to enqueue the task.  If none
     *  available, wakeup one.
     */
    Worker* worker = NULL;
    uint64_t index = 0;
    uint64_t workerIdx = 0;
    uint64_t numActiveWorkers = mNumActiveWorkers;
    if (numActiveWorkers > 0) {
        index = __sync_fetch_and_add(&mRoundRabinIndex, 1);
        workerIdx = index % numActiveWorkers;
        worker = mActiveList.at(workerIdx);
        //printf("rr index: %lu, worker %lu, countdown:%lu, numWorkers:%lu\n", mRoundRabinIndex, worker->getId(), mThreadPinningCountDown, numActiveWorkers);
    } else {
        const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);
        uint64_t currentTime = RAMCloud::Cycles::rdtsc();
        mNextAdjustCycle = currentTime +
            RAMCloud::Cycles::fromMicroseconds(mAdjustmentIntervalUs);
        if (mNumActiveWorkers > 0) {
            worker = mActiveList.front();
        } else {
            worker = mIdleList.back();
            mIdleList.pop_back();
            while (!worker->goActive());
            mActiveList.push_back(worker);
            mNumActiveWorkers++;
        }
    }
    assert(worker);
    if (!worker->enqueue(task)) {
        RAMCLOUD_LOG(NOTICE, "enqueue failed at worker %lu", worker->getId());
        return enqueue(task);
    }
    /* 2. perform worker management.  When there are >=1 workers,
     * if average tasks list size is below the low watermark, spindown
     * one
     */
    uint64_t currentTime = RAMCloud::Cycles::rdtsc();
    if (currentTime > mNextAdjustCycle) {
        const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);
        mNextAdjustCycle = currentTime +
            RAMCloud::Cycles::fromMicroseconds(mAdjustmentIntervalUs);

        if (mNumActiveWorkers > 0) {
            worker = mActiveList.back();
            /*
             * Spinup the workers
             */
            //double avgQueueLength = getAvgTaskQueuesLength(false);
            //if (avgQueueLength > mHighWaterMark) {
            if (worker->getTaskQueueLength() > mHighWaterMark) {
                //Spinup the number of active workers
                if (mIdleList.size() > 0) {
                    worker = mIdleList.back();
                    if (worker->goActive()) {
                        mIdleList.pop_back();
                        mActiveList.push_back(worker);
                        mNumActiveWorkers++;
                        RAMCLOUD_LOG(NOTICE, "worker %lu go active", worker->getId());
                    }
                }
                return;
            }
            /*
             * Spindown the workers
             */
            worker = mActiveList.front();
            //if ((avgQueueLength <= mLowWaterMark) &&
            if ((worker->getTaskQueueLength() <= mLowWaterMark) &&
                (mNumActiveWorkers > mMinActiveWorkers)) {
                worker = mActiveList.back();
                if (worker->goIdle()) {
                    mNumActiveWorkers--;
                    mActiveList.pop_back();
                    mIdleList.push_back(worker);
                    RAMCLOUD_LOG(NOTICE, "worker %lu go to sleep", worker->getId());
                }
            }
        }
    }
}

}
