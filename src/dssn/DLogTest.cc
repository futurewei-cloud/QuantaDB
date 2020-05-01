/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "DLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class DLogTest : public ::testing::Test {
  public:
  DLogTest() {};
  ~DLogTest() {};

  DLog<256> log;

  DISALLOW_COPY_AND_ASSIGN(DLogTest);
};

TEST_F(DLogTest, DLogUnitTest) {

    uint32_t dsize = log.size();
    uint32_t fsize = log.free_space();
    GTEST_COUT << "DLogTest dsize; " << dsize << " fsize:" << fsize << std::endl; 

    for(uint32_t idx = 0; idx < 1024; idx++) {
        uint64_t off = log.append("abcdefgh", 8);
        EXPECT_EQ(off, dsize + (idx * 8));
    }

    log.set_chunk_size(1024);

    dsize = log.size();
    for(uint32_t idx = 0; idx < 1024; idx++) {
        uint64_t off = log.append("abcdefgh", 8);
        EXPECT_EQ(off, dsize + (idx * 8));
    }

    log.trim(10240);
}

TEST_F(DLogTest, benchDLog) {
#if (0)
    int loop = 1024*1024;
    uint64_t start, stop;
    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i++) {
        clock.getClusterTime();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getClusterTime: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec per call " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i++) {
        clock.getClusterTime(1000);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getClusterTime(delta): "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec per call " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i++) {
        clock.getLocalTime();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "getLocalTime: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec per call " << std::endl;

    //
    start = Cycles::rdtsc();
    for (int i = 0; i < loop; i++) {
        clock.Cluster2Local(start);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Cluster2Local: "
    << Cycles::toNanoseconds(stop - start)/(1024*1024) << " nano sec per call " << std::endl;
#endif
}
}  // namespace RAMCloud
