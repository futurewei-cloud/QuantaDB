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

#include "TestUtil.h"
#include "ClusterTimeService.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

class ClusterTimeServiceTest : public ::testing::Test {
  public:
  ClusterTimeServiceTest() {};
  ~ClusterTimeServiceTest() {};

  ClusterTimeService clock, clock1, clock2, clock3, clock4;
  std::atomic<uint64_t> cntr;

  DISALLOW_COPY_AND_ASSIGN(ClusterTimeServiceTest);
};

TEST_F(ClusterTimeServiceTest, getLocalTime) {
    for(int ii = 0; ii < 100; ii++) {
        uint64_t t1 = clock.getLocalTime();
        uint64_t t2 = clock.getLocalTime();
        uint64_t t3 = clock.getLocalTime();
        uint64_t t4 = clock.getLocalTime();
        EXPECT_GT(t2, t1);
        EXPECT_GT(t3, t2);
        EXPECT_GT(t4, t3);
    }
}

TEST_F(ClusterTimeServiceTest, multiClockTest) {
    for(int ii = 0; ii < 1024*1024; ii++) {
        uint64_t t0 = clock.getLocalTime();
        uint64_t t1 = clock1.getLocalTime();
        uint64_t t2 = clock2.getLocalTime();
        uint64_t t3 = clock3.getLocalTime();
        uint64_t t4 = clock4.getLocalTime();
        EXPECT_GT(t1, t0);
        EXPECT_GT(t2, t1);
        EXPECT_GT(t3, t2);
        EXPECT_GT(t4, t3);
    }
}

void multiThreadTest(ClusterTimeServiceTest *t)
{
    for(int ii = 0; ii < 1024*1024; ii++) {
        uint64_t tA = t->clock.getLocalTime();
        uint64_t tB = t->clock1.getLocalTime();
        uint64_t tC = t->clock2.getLocalTime();
        uint64_t tD = t->clock3.getLocalTime();
        uint64_t tE = t->clock4.getLocalTime();
        EXPECT_GT(tB, tA);
        EXPECT_GT(tC, tB);
        EXPECT_GT(tD, tC);
        EXPECT_GT(tE, tD);
    }
}

TEST_F(ClusterTimeServiceTest, MTClockTest) {
    std::thread t1(multiThreadTest, this);
    std::thread t2(multiThreadTest, this);
    std::thread t3(multiThreadTest, this);

    t1.join();
    t2.join();
    t3.join();
}

#define OP_WAIT     0
#define OP_START    1
#define OP_END      2
void MTAtomicIntInc(ClusterTimeServiceTest *t, int *op)
{
    while (*op == OP_WAIT);
    while (*op != OP_END)
        t->cntr++;
}

TEST_F(ClusterTimeServiceTest, BenchMTAtomicInc) {
    uint64_t start, stop;
    int op = OP_WAIT;

    cntr = 0;
    std::thread t1(MTAtomicIntInc, this, &op);
    std::thread t2(MTAtomicIntInc, this, &op);
    std::thread t3(MTAtomicIntInc, this, &op);
    std::thread t4(MTAtomicIntInc, this, &op);
    std::thread t5(MTAtomicIntInc, this, &op);

    start = Cycles::rdtsc();
    op = OP_START;
    sleep(1);
    op = OP_END;
    stop = Cycles::rdtsc();
    GTEST_COUT << "MT Bench Atomic Inc: "
    << Cycles::toNanoseconds(stop - start) / cntr << " nano sec " << std::endl;

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
}

TEST_F(ClusterTimeServiceTest, benchGenClusterTime) {
    int loop = 1; // 1024*1024;
    uint64_t start, stop;
 
    start = Cycles::rdtscp();
    clock.getLocalTime();
    stop = Cycles::rdtscp();
    GTEST_COUT << "1st getLocalTime: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec " << std::endl;

    start = Cycles::rdtscp();
    clock.getLocalTime();
    stop = Cycles::rdtscp();
    GTEST_COUT << "2nt getLocalTime: "
    << Cycles::toNanoseconds(stop - start) << " nano sec " << std::endl;

    start = Cycles::rdtscp();
    clock.getLocalTime();
    stop = Cycles::rdtscp();
    GTEST_COUT << "3rd getLocalTime: "
    << Cycles::toNanoseconds(stop - start) << " nano sec " << std::endl;

    start = Cycles::rdtscp();
    clock.getLocalTime();
    stop = Cycles::rdtscp();
    GTEST_COUT << "4th getLocalTime: "
    << Cycles::toNanoseconds(stop - start) << " nano sec " << std::endl;
}

}  // namespace RAMCloud
