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
  ~DLogTest() { delete log; };

  DLog<256> * log = new DLog<256>("/dev/shm", false); // Use small chunk size to stress boundary condition.

  DISALLOW_COPY_AND_ASSIGN(DLogTest);
};

class DLogBench : public ::testing::Test {
  public:
  DLogBench() {};
  ~DLogBench() { delete log; };

  #define DLOG_CHUNK_SIZE (uint64_t)1024*1024*1024*100
  #define DLOG_DIR        "/dev/shm"
  DLog<DLOG_CHUNK_SIZE> * log = new DLog<DLOG_CHUNK_SIZE>(DLOG_DIR, false);

  DISALLOW_COPY_AND_ASSIGN(DLogBench);
};

class MemBench : public ::testing::Test {
  public:
  MemBench() {};
  ~MemBench() { delete log; };

  #define MEM_LOG_SIZE (uint64_t)1024*1024*1024*100
  char * log = new char[MEM_LOG_SIZE];

  DISALLOW_COPY_AND_ASSIGN(MemBench);
};

TEST_F(DLogTest, DLogUnitTest)
{
    uint32_t dsize = log->size();

    for(uint32_t idx = 0; idx < 1024; idx++) {
        uint64_t off = log->append("abcdefgh", 8);
        EXPECT_EQ(off, dsize + (idx * 8));
    }

    log->set_chunk_size(1024);

    dsize = log->size();
    for(uint32_t idx = 0; idx < 1024; idx++) {
        uint64_t off = log->append("abcdefgh", 8);
        EXPECT_EQ(off, dsize + (idx * 8));
    }

    dsize = log->size();

    char buf[8];
    for (uint64_t off = 0; off < dsize; off += sizeof(buf) ) {
        uint32_t nrd = log->read(off, buf, sizeof(buf));
        EXPECT_EQ(nrd, sizeof(buf));
        EXPECT_EQ("abcdefgh", std::string(buf, 8));
    }

    log->trim(0); // trim all
}

TEST_F(DLogBench, DLogBench) {
    uint32_t loop = 1024*1024;
    uint64_t start, stop;
    //
    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 10) {
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
        log->reserve(1);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "DLog.reserve latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec" << std::endl;

    //
    start = Cycles::rdtsc();
    char buf[1024];
    loop = 1024*1024*100;
    for (uint32_t i = 0; i < loop; i++) {
        log->append(buf, sizeof(buf));
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toMicroseconds(stop - start);
    float    gbps = (float)((sizeof(buf)*loop)/msec) / 1024;
    std::cerr << std::fixed;
    GTEST_COUT << "DLog Append: log dir: " << DLOG_DIR << ", log fsize: " << DLOG_CHUNK_SIZE/(1024*1024) << " MB" << std::endl;
    GTEST_COUT << "DLog.append throughput: " << gbps << " GB/sec" << std::endl;
}

TEST_F(MemBench, MemBench) {
    uint64_t start, stop;
    char buf[1024];

    // 1st round, would be slower bcz of page-fault
    start = Cycles::rdtsc();
    for (uint64_t off = 0; off < MEM_LOG_SIZE; off += sizeof(buf)) {
        memcpy(&log[off], buf, sizeof(buf)); 
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toMicroseconds(stop - start);
    float    gbps = (float)(MEM_LOG_SIZE/msec) / 1024;
    std::cerr << std::fixed;
    GTEST_COUT << "Mem append throughput (1st round): " << gbps << " GB/sec" << std::endl;

    // 2nd round.
    start = Cycles::rdtsc();
    for (uint64_t off = 0; off < MEM_LOG_SIZE; off += sizeof(buf)) {
        memcpy(&log[off], buf, sizeof(buf)); 
    }
    stop = Cycles::rdtsc();
    msec = Cycles::toMicroseconds(stop - start);
    gbps = (float)(MEM_LOG_SIZE/msec) / 1024;
    std::cerr << std::fixed;
    GTEST_COUT << "Mem append throughput (2nd round): " << gbps << " GB/sec" << std::endl;
}

}  // namespace RAMCloud
