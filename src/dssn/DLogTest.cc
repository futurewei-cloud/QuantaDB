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
  ~DLogTest() {
    log.trim(log.size());
  };

  DLog<256> log; // Use small chunk size to stress boundary condition.

  DISALLOW_COPY_AND_ASSIGN(DLogTest);
};

TEST_F(DLogTest, DLogUnitTest)
{
    uint32_t dsize = log.size();

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

    dsize = log.size();

    char buf[8];
    for (uint64_t off = 0; off < dsize; off += sizeof(buf) ) {
        uint32_t nrd = log.read(off, buf, sizeof(buf));
        EXPECT_EQ(nrd, sizeof(buf));
        EXPECT_EQ("abcdefgh", std::string(buf, 8));
    }

    log.trim(0); // trim all

}

TEST_F(DLogTest, DLogBench) {
    log.set_chunk_size(1024*1024*64);

    uint32_t loop = 1024*1024;
    uint64_t start, stop;
    //
    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 10) {
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
        log.reserve(1);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "log.reserve latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec" << std::endl;

    //
    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i++) {
        char buf[1024*4];
        log.append(buf, sizeof(buf));
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toNanoseconds(stop - start)/(1000*1000);
    float    gbps = (float)(1024*4)/msec;
    std::cerr << std::fixed;
    GTEST_COUT << "log.append throughput: " << gbps << " GB/sec" << std::endl;

#if (0)
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
