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
#include "DLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

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
  ~DLogBench() { delete log; delete log2; };

  #define DLOG_CHUNK_SIZE (uint64_t)1024*1024*1024*100
  #define DLOG_DIR        "/dev/shm"
  DLog<DLOG_CHUNK_SIZE> * log = new DLog<DLOG_CHUNK_SIZE>(DLOG_DIR, false);

  #define DLOG2_CHUNK_SIZE (uint64_t)1024*1024*1024*100
  #define DLOG2_DIR        "/dev/shm/2"
  DLog<DLOG2_CHUNK_SIZE> * log2 = new DLog<DLOG2_CHUNK_SIZE>(DLOG2_DIR, false);

  DISALLOW_COPY_AND_ASSIGN(DLogBench);
};

class DLogMemBench : public ::testing::Test {
  public:
  DLogMemBench() {};
  ~DLogMemBench() { delete log; };

  #define MEM_LOG_SIZE (uint64_t)1024*1024*1024*100
  char * log = new char[MEM_LOG_SIZE];

  DISALLOW_COPY_AND_ASSIGN(DLogMemBench);
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
    EXPECT_EQ(log->size(), (uint64_t)0);
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
    loop = 1024*1024*10;
    for (uint32_t i = 0; i < loop; i++) {
        log->append(buf, sizeof(buf));
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toMicroseconds(stop - start);
    float    gbps = (float)((sizeof(buf)*loop)/msec) / 1024;
    std::cerr << std::fixed;
    GTEST_COUT << "DLog.append throughput: " << gbps << " GB/sec"
               << " (logdir:" << DLOG_DIR << ", logsize: " << DLOG_CHUNK_SIZE/(1024*1024) << " MB,"
               << " copy size:" << loop*sizeof(buf)/(1024*1024) << " MB)" << std::endl;
}

void writeToLog(DLogBench *c, float *gbps /* out */)
{
    char buf[1000];
    uint64_t start, stop;
    uint64_t loop = 1024*1024*100;
    start = Cycles::rdtsc();
    for (uint64_t i = 0; i < loop; i++) {
        c->log->append(buf, sizeof(buf));
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toMicroseconds(stop - start);
    *gbps = (float)((sizeof(buf)*loop)/msec) / 1024;
}

void writeToLog2(DLogBench *c, float *gbps /* out */)
{
    char buf1[50], buf2[50];
    uint64_t start, stop;
    uint64_t loop = 1024*1024*100;
    start = Cycles::rdtsc();
    for (uint64_t i = 0; i < loop; i++) {
        c->log2->append(buf1, sizeof(buf1));
        c->log2->append(buf2, sizeof(buf2));
    }
    stop = Cycles::rdtsc();
    uint64_t msec = Cycles::toMicroseconds(stop - start);
    *gbps = (float)(((sizeof(buf1)+sizeof(buf2))*loop)/msec) / 1024;
}

#if (0) // Disable to shorten test time
TEST_F(DLogBench, DLogMultiBench) {
    float gbps1 = 0, gbps2 = 0;

    std::thread t1(writeToLog, this, &gbps1);
    std::thread t2(writeToLog2, this, &gbps2);

    t1.join();
    t2.join();

    GTEST_COUT << "DLogMultiBench: Two threads. T1 does 100M append of 1000 bytes. T2, 200M append of 50 bytes" << std::endl;
    GTEST_COUT << "DLogMultiBench T1 thruput: " << gbps1 << "GB/sec. T2: " << gbps2 << "GB/sec" << std::endl;

}

TEST_F(DLogMemBench, MemBench) {
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
#endif // 0

}  // namespace RAMCloud
