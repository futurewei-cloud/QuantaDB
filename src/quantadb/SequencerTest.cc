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
#include "Sequencer.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

class SequencerTest : public ::testing::Test {
  public:
  SequencerTest() {};
  ~SequencerTest() {};

  Sequencer seq;

  DISALLOW_COPY_AND_ASSIGN(SequencerTest);
};

TEST_F(SequencerTest, getCTS) {
    GTEST_COUT << "SequencerTest" << std::endl;
    __uint128_t cts1 = seq.getCTS();
    __uint128_t cts2 = seq.getCTS();
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
