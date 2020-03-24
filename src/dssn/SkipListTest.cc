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
  #define LOOP    1024*1024
  uint64_t loop = LOOP;
  char *buf[LOOP];

  SkiplistTest()
  {
    // setup buf[]
    for (uint64_t i = 0; i < loop; ++i){
        buf[i] = (char *)malloc(32);
        assert(buf[i]);
        sprintf(buf[i], "%ld", i);
    }
  };

  ~SkiplistTest()
  {
    for (uint64_t i = 0; i < loop; ++i){
        free(buf[i]);
    }
  };

  SkipList s;

  SkipList *sp = new SkipList(0.2);

  DISALLOW_COPY_AND_ASSIGN(SkiplistTest);
};


TEST_F(SkiplistTest, unit_test) {
    void * ret;

    ret = s.get();
    EXPECT_EQ(ret, nullptr);
    ret = s.get(10);
    EXPECT_EQ(ret, nullptr);

    s.insert(10, buf[10]);
    ret = s.get(10);
    EXPECT_EQ(ret, buf[10]);
    s.remove(10);
    ret = s.get(10);
    EXPECT_EQ(ret, nullptr);

    loop = 1024;

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

    GTEST_COUT << "SkipList unit test done" << std::endl;
}

TEST_F(SkiplistTest, benchGetCTS) {
    uint64_t start, stop;

    GTEST_COUT << "Skiplist entry=" << loop << " maxL=" << s.maxLevel << " prob=" << s.probability << std::endl;

    // 1. insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(i, buf[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist insert:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    fflush(stdout);

    // 2. get(key)
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get(i);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get(key):"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;
    fflush(stdout);

    // 3. get()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;
    fflush(stdout);

    // 4. pop()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.pop();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist pop():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;
    fflush(stdout);

}

}  // namespace RAMCloud
