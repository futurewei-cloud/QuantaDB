/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "ClusterTimeService.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class ClusterTimeServiceTest : public ::testing::Test {
  public:
  ClusterTimeServiceTest() {};
  ~ClusterTimeServiceTest() {};

  ClusterTimeService clock, clock1, clock2, clock3, clock4;

  DISALLOW_COPY_AND_ASSIGN(ClusterTimeServiceTest);
};

TEST_F(ClusterTimeServiceTest, getClusterTime) {
    for(int ii = 0; ii < 1000; ii++) {
        uint64_t t1 = clock.getClusterTime();
        uint64_t t2 = clock.getClusterTime();
        uint64_t t3 = clock.getClusterTime();
        uint64_t t4 = clock.getClusterTime();
        EXPECT_GT(t2, t1);
        EXPECT_GT(t3, t2);
        EXPECT_GT(t4, t3);
    }
}

TEST_F(ClusterTimeServiceTest, getLocalTime) {
    for(int ii = 0; ii < 1000; ii++) {
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
    for(int ii = 0; ii < 1000; ii++) {
        uint64_t t0 = clock.getClusterTime();
        uint64_t t1 = clock1.getClusterTime();
        uint64_t t2 = clock2.getClusterTime();
        uint64_t t3 = clock3.getClusterTime();
        uint64_t t4 = clock4.getClusterTime();
        EXPECT_GT(t1, t0);
        EXPECT_GT(t2, t1);
        EXPECT_GT(t3, t2);
        EXPECT_GT(t4, t3);
    }
}

void multiThreadTest(ClusterTimeServiceTest *t)
{
    for(int ii = 0; ii < 1000; ii++) {
        uint64_t tA = t->clock.getClusterTime();
        uint64_t tB = t->clock1.getClusterTime();
        uint64_t tC = t->clock2.getClusterTime();
        uint64_t tD = t->clock3.getClusterTime();
        uint64_t tE = t->clock4.getClusterTime();
        EXPECT_GT(tB, tA);
        EXPECT_GT(tC, tB);
        EXPECT_GT(tD, tC);
        EXPECT_GT(tE, tD);
    }
}

TEST_F(ClusterTimeServiceTest, multiThreadClockTest) {
    std::thread t1(multiThreadTest, this);
    std::thread t2(multiThreadTest, this);
    std::thread t3(multiThreadTest, this);

    t1.join();
    t2.join();
    t3.join();
}

TEST_F(ClusterTimeServiceTest, benchGenClusterTime) {
    int loop = 1024*1024;
    uint64_t start, stop;
    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i += 10) {
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
        clock.getClusterTime();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getClusterTime: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i += 10) {
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
        clock.getClusterTime(1000);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getClusterTime(delta): "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i += 10) {
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
        clock.getLocalTime();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getLocalTime: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i += 10) {
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
        clock.Cluster2Local(start);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Cluster2Local: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec " << std::endl;

}

}  // namespace RAMCloud
