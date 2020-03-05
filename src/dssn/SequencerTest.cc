/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "Sequencer.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class SequencerTest : public ::testing::Test {
  public:
  SequencerTest() {};
  ~SequencerTest() {};

  Sequencer seq;

  DISALLOW_COPY_AND_ASSIGN(SequencerTest);
};

TEST_F(SequencerTest, getCTS) {
    GTEST_COUT << "SequencerTest" << std::endl;
    uint64_t cts1 = seq.getCTS();
    uint64_t cts2 = seq.getCTS();
    EXPECT_GT(cts2, cts1);
}

TEST_F(SequencerTest, benchGetCTS) {
    int loop = 1024*1024;
    uint64_t start, stop;
    //
    start = Cycles::rdtscp();
    for (int i = 0; i < loop; i++) {
        seq.getCTS();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "getCTS: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

}
}  // namespace RAMCloud
