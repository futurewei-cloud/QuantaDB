/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "TxLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class TxLogTest : public ::testing::Test {
  public:
  TxLogTest() {};
  ~TxLogTest() {
  };

  TxLog txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogTest);
};

TEST_F(TxLogTest, TxLogUnitTest)
{
    GTEST_COUT << "TxLogTest" << std::endl;
}

#if (0)
TEST_F(TxLogTest, TxLogBench) {
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

}
#endif
}  // namespace RAMCloud
