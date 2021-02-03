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
#include "Slab.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

#define SLAB_MAG_SIZE (1024*1024*16)

class SlabTest : public ::testing::Test {
  public:
  SlabTest() {};
  ~SlabTest() { delete slab; };

  Slab<256> * slab = new Slab<256>(32); // Use small chunk size to stress boundary condition.

  DISALLOW_COPY_AND_ASSIGN(SlabTest);
};

class SlabBench : public ::testing::Test {
  public:
  SlabBench() {};
  ~SlabBench() { delete slab; };

  Slab<SLAB_MAG_SIZE> * slab = new Slab<SLAB_MAG_SIZE>(32); // Use small chunk size to stress boundary condition.

  DISALLOW_COPY_AND_ASSIGN(SlabBench);
};

TEST_F(SlabTest, SlabUnitTest)
{
    uint32_t loop = 1024*1024;
    void ** foo = (void**)malloc(sizeof(void*) * loop);
    EXPECT_EQ(slab->count_free(), (uint32_t)256);
    for(uint32_t idx = 0; idx < loop; idx++) {
        void * ptr = slab->get();
        foo[idx] = ptr;
        bzero(ptr, 32);
    }
    for(uint32_t idx = 0; idx < loop; idx++) {
        slab->put(foo[idx]);
    }
    EXPECT_EQ(slab->count_free(), (uint32_t)1024*1024);
    free (foo);
}

TEST_F(SlabBench, SlabBench)
{
    uint32_t loop = 1024*1024;
    uint64_t start, stop;
    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 10) {
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
        slab->get();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Slab get latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec "
    << " free cnt: " << slab->count_free() 
    << std::endl;

    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 10) {
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
        malloc(32);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Malloc(32) latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec" << std::endl;
}

}  // namespace RAMCloud
