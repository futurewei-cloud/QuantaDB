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

#include "TestUtil.h"
#include "TestLog.h"
#include "WorkerPool.h"


using namespace QDB;
using namespace RAMCloud;

class WorkerPoolTest : public ::testing::Test {
public:
    TestLog::Enable logEnabler;
    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;
    WorkerPool* wp;
    std::atomic<uint64_t> counter;
    void Increment(std::atomic<uint64_t>* c) {
        Cycles::sleep(taskExecTimeUs);
        (*c)++;
    }
    explicit WorkerPoolTest()
    {
        wp = new WorkerPool(numWorkers, minWorkers,
                            highWatermark, lowWatermark);
        counter = 0;
    }
    const uint64_t numWorkers = 16;
    const uint64_t minWorkers = 2;
    const uint64_t highWatermark = 10;
    const uint64_t lowWatermark = 2;
    const uint64_t taskExecTimeUs = 100;
    DISALLOW_COPY_AND_ASSIGN(WorkerPoolTest);
};

TEST_F(WorkerPoolTest, basic) {
    Task* task = wp->allocTask();
    task->callback = std::bind(&WorkerPoolTest::Increment,
                               this,&counter);
    wp->enqueue(task);
    while(!wp->isTaskQueuesEmpty());
    EXPECT_EQ(counter, 1);
}

TEST_F(WorkerPoolTest, multitasks) {
    uint64_t numIncrTasks = highWatermark;
    for (uint64_t i = 0; i < numIncrTasks; i++) {
        Task* task = wp->allocTask();
        task->callback = std::bind(&WorkerPoolTest::Increment,
                                   this,&counter);
        wp->enqueue(task);
    }
    EXPECT_EQ(wp->getNumActiveWorkers(), 1);
    while(!wp->isTaskQueuesEmpty());
    EXPECT_EQ(counter, numIncrTasks);
}

TEST_F(WorkerPoolTest, spinup) {
    uint64_t numIncrTasks = 20;
    for (uint64_t i = 0; i < numIncrTasks; i++) {
        Task* task = wp->allocTask();
        task->callback = std::bind(&WorkerPoolTest::Increment,
                                   this,&counter);
        wp->enqueue(task);
        Cycles::sleep(5);
    }
    EXPECT_TRUE(wp->getNumActiveWorkers()>= 2);
    EXPECT_TRUE(wp->getNumIdleWorkers() <= (numWorkers-2));
    while(!wp->isTaskQueuesEmpty());
    EXPECT_EQ(counter, 20);
}

TEST_F(WorkerPoolTest, spindown) {
    uint64_t numIncrTasks = highWatermark+2;
    for (uint64_t i = 0; i < numIncrTasks; i++) {
        Task* task = wp->allocTask();
        task->callback = std::bind(&WorkerPoolTest::Increment,
                                   this,&counter);
        wp->enqueue(task);
        Cycles::sleep(5);
    }
    while(!wp->isTaskQueuesEmpty());
    for (uint64_t i = 0; i < 1; i++) {
        Task* task = wp->allocTask();
        task->callback = std::bind(&WorkerPoolTest::Increment,
                                   this,&counter);
        wp->enqueue(task);
        Cycles::sleep(5);
    }
    EXPECT_TRUE(wp->getNumActiveWorkers() <= minWorkers);
    EXPECT_TRUE(wp->getNumIdleWorkers()>= (numWorkers-minWorkers));
    while(!wp->isTaskQueuesEmpty());
    EXPECT_EQ(counter, 13);
}

TEST_F(WorkerPoolTest, execlatency) {
    uint64_t numIncrTasks = 1000;
    uint64_t start = Cycles::rdtsc();
    for (uint64_t i = 0; i < numIncrTasks; i++) {
        Task* task = wp->allocTask();
        task->callback = std::bind(&WorkerPoolTest::Increment,
                                   this,&counter);
        wp->enqueue(task);
        Cycles::sleep(10);
    }

    EXPECT_TRUE(wp->getNumActiveWorkers() >= 3);
    EXPECT_TRUE(wp->getNumIdleWorkers() <= (numWorkers-3));
    while(!wp->isTaskQueuesEmpty());
    uint64_t end = Cycles::rdtsc();
    EXPECT_EQ(counter, numIncrTasks);
    EXPECT_TRUE(Cycles::toMicroseconds(end-start) < (((taskExecTimeUs+10)*numIncrTasks)>>1));
    printf("execution time: %lu vs %lu\n", Cycles::toMicroseconds(end-start),
           ((taskExecTimeUs+10)*numIncrTasks)>>1);
    printf("spinup latency avg/max(us): %lu/%lu\n", wp->getAvgSpinupLatencyUs(),
           wp->getMaxSpinupLatencyUs());
}
