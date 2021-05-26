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

namespace QDB {

void
Worker::workerTask()
{
    while (1) {
        Task* task = mTasks.get();
        if (task != NULL) {
            task->callback();
            mWorkerPool->freeTask(task);
            mNumTasks--;
        } else if((mNumTasks == 0) && (mAction == WORKER_GO_IDLE)) {
            mAction = WORKER_ACTION_RESET;
            std::unique_lock<std::mutex> lock(mMtx);
            mCV.wait(lock);
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
            }
        } else if ((mAction == WORKER_GO_EXIT)) {
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
    uint64_t numActiveWorkers = mNumActiveWorkers;
    Worker* worker = NULL;
    if (numActiveWorkers > 0) {
        uint64_t index = 0;
        uint64_t workerIdx = 0;
        if (mThreadPinningCountDown > 0) {
            workerIdx = mPinnedThreadIdx;
            mThreadPinningCountDown--;
        } else {
            index = __sync_fetch_and_add(&mRoundRabinIndex, 1);
            workerIdx = index % numActiveWorkers;
        }
        worker = mActiveList.at(workerIdx);
        //printf("rr index: %lu, worker %lu, countdown:%lu, numWorkers:%lu\n", mRoundRabinIndex, worker->getId(), mThreadPinningCountDown, numActiveWorkers);
    } else {
        const std::lock_guard<std::mutex> lock(mWorkerMgmtLock);
        if (mNumActiveWorkers > 0) {
            worker = mActiveList.front();
        } else {
            worker = mIdleList.back();
            mIdleList.pop_back();
            worker->goActive();
            mActiveList.push_back(worker);
            mNumActiveWorkers++;
        }
    }
    assert(worker);
    if (!worker->enqueue(task)) {
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

            if ((mThreadPinningCountDown == 0) &&
                (worker->getTaskQueueLength() > mHighWaterMark)) {
                //Spinup the number of active workers
                if (mIdleList.size() > 0) {
                    /*
                      Absorb incoming load up to 1/2 of the oldest worker
                      worker = mActiveList.front();
                      uint64_t pinCount = worker->getTaskQueueLength() >> 1;
                    */
                    uint64_t pinCount = mHighWaterMark - 1;
                    worker = mIdleList.back();
                    mIdleList.pop_back();
                    worker->goActive();
                    mActiveList.push_back(worker);
                    mPinnedThreadIdx = mActiveList.size() - 1;
                    mThreadPinningCountDown = pinCount;
                    mNumActiveWorkers++;
                }
                return;
            }

            worker = mActiveList.front();
            if ((worker->getTaskQueueLength() < mLowWaterMark) &&
                (mNumActiveWorkers > mMinActiveWorkers)) {
                //Spindown the number of active workers
                mNumActiveWorkers--;
                worker = mActiveList.back();
                mActiveList.pop_back();
                worker->goIdle();
                mIdleList.push_back(worker);
            }
        }
    }
}

}
