/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "SkipList.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class SkiplistTest : public ::testing::Test {
  public:
  SkiplistTest() {};
  ~SkiplistTest() {};

  SkipList s;

  DISALLOW_COPY_AND_ASSIGN(SkiplistTest);
};

TEST_F(SkiplistTest, unit_test) {
    uint64_t loop = 1024;
    char buf[loop][32];

    // 0. measure overhead
    for (uint64_t i = 0; i < loop; ++i){
        sprintf(buf[i], "%ld", i);
    }
    //
    // 1. insert()
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(i, buf[i]);
    }

    // 2. get(key)
    for (uint64_t i = 0; i < loop; ++i){
        void *ret = s.get(i);
        EXPECT_EQ(ret, buf[i]);
    }

    // 3. get()
    for (uint64_t i = 0; i < loop; ++i){
        void *ret = s.get();
        EXPECT_EQ(ret, buf[0]);
    }

    // 4. erase
    for (uint64_t i = 100; i < 200; ++i){
        s.remove(i);
    }

    // 5. get()
    for (uint64_t i = 0; i < loop; ++i){
        void *ret = s.get(i);
        if (i < 100 || i >= 200)
            EXPECT_EQ(ret, buf[i]);
        else
            EXPECT_EQ(ret, (void *)NULL);
    }

    // 4. pop()
    for (uint64_t i = 0; i < loop; ++i){
        s.pop();
    }
}

TEST_F(SkiplistTest, benchGetCTS) {
    uint64_t loop = 100;
    uint64_t start, stop;
    char buf[loop][32];

    // 0. measure overhead
    for (uint64_t i = 0; i < loop; ++i){
        sprintf(buf[i], "%ld", i);
    }
    //
    // 1. insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(i, buf[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist insert:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // 2. get(key)
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get(i);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get(key):"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // 3. get()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // 4. pop()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.pop();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist pop():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;


    GTEST_COUT << "Skiplist done" << std::endl;
}

}  // namespace RAMCloud
