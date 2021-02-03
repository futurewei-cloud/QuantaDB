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

class SlabTest : public ::testing::Test {
  public:
  SlabTest() {};
  ~SlabTest() { delete slab; };

  Slab *slab = new Slab(32, 256); // Use small magacap size to stress boundary condition.

  DISALLOW_COPY_AND_ASSIGN(SlabTest);
};

class SlabBench : public ::testing::Test {
  public:
  SlabBench() {};
  ~SlabBench() { delete slab; };

  #define SLAB_MAG_SIZE (1024*1024*16)
  Slab * slab = new Slab(32, SLAB_MAG_SIZE); // Large magacap for benchmark 

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
    void ** foo = (void**)malloc(sizeof(void*) * loop);
    uint64_t start, stop;
    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        foo[i+0] = slab->get();
        foo[i+1] = slab->get();
        foo[i+2] = slab->get();
        foo[i+3] = slab->get();
        foo[i+4] = slab->get();
        foo[i+5] = slab->get();
        foo[i+6] = slab->get();
        foo[i+7] = slab->get();
        foo[i+8] = slab->get();
        foo[i+9] = slab->get();
        foo[i+10] = slab->get();
        foo[i+11] = slab->get();
        foo[i+12] = slab->get();
        foo[i+13] = slab->get();
        foo[i+14] = slab->get();
        foo[i+15] = slab->get();
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Slab get latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nsec "
    << " free cnt: " << slab->count_free() 
    << std::endl;

    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        slab->put(foo[i+0]);
        slab->put(foo[i+1]);
        slab->put(foo[i+2]);
        slab->put(foo[i+3]);
        slab->put(foo[i+4]);
        slab->put(foo[i+5]);
        slab->put(foo[i+6]);
        slab->put(foo[i+7]);
        slab->put(foo[i+8]);
        slab->put(foo[i+9]);
        slab->put(foo[i+10]);
        slab->put(foo[i+11]);
        slab->put(foo[i+12]);
        slab->put(foo[i+13]);
        slab->put(foo[i+14]);
        slab->put(foo[i+15]);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Slab put latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nsec "
    << " free cnt: " << slab->count_free() 
    << std::endl;

    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        foo[i+0] = malloc(32);
        foo[i+1] = malloc(32);
        foo[i+2] = malloc(32);
        foo[i+3] = malloc(32);
        foo[i+4] = malloc(32);
        foo[i+5] = malloc(32);
        foo[i+6] = malloc(32);
        foo[i+7] = malloc(32);
        foo[i+8] = malloc(32);
        foo[i+9] = malloc(32);
        foo[i+10] = malloc(32);
        foo[i+11] = malloc(32);
        foo[i+12] = malloc(32);
        foo[i+13] = malloc(32);
        foo[i+14] = malloc(32);
        foo[i+15] = malloc(32);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Malloc(32) latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nsec" << std::endl;

    start = Cycles::rdtsc();
    for (uint32_t i = 0; i < loop; i += 16) {
        free(foo[i+0]);
        free(foo[i+1]);
        free(foo[i+2]);
        free(foo[i+3]);
        free(foo[i+4]);
        free(foo[i+5]);
        free(foo[i+6]);
        free(foo[i+7]);
        free(foo[i+8]);
        free(foo[i+9]);
        free(foo[i+10]);
        free(foo[i+11]);
        free(foo[i+12]);
        free(foo[i+13]);
        free(foo[i+14]);
        free(foo[i+15]);
    }
    stop = Cycles::rdtsc();
    GTEST_COUT << "Free latency: "
    << Cycles::toNanoseconds(stop - start)/loop << " nsec " << std::endl;

    free (foo);
}

}  // namespace RAMCloud
