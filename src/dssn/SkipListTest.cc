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
  uint32_t randkey[LOOP];

  SkiplistTest()
  {
    // setup buf[]
    for (uint64_t i = 0; i < loop; ++i){
        buf[i] = (char *)malloc(32);
        assert(buf[i]);
        sprintf(buf[i], "%ld", i);
    }

    //
    for(uint64_t idx = 0; idx < loop; idx++) {
        randkey[idx] = std::rand()%LOOP;
    }
  };

  ~SkiplistTest()
  {
    for (uint64_t i = 0; i < loop; ++i){
        free(buf[i]);
    }
  };

  SkipList<uint64_t> s;

  // SkipList<uint64_t> *sp = new SkipList<uint64_t>(0.2);

  DISALLOW_COPY_AND_ASSIGN(SkiplistTest);
};


TEST_F(SkiplistTest, unit_test) {
    void * ret;

    ret = s.get();
    EXPECT_EQ(ret, nullptr);
    ret = s.get(10);
    EXPECT_EQ(ret, nullptr);

    s.insert(10, buf[10]);
    s.insert(11, buf[11]);
    ret = s.get(10);
    EXPECT_EQ(ret, buf[10]);
    ret = s.get(11);
    EXPECT_EQ(ret, buf[11]);

    s.remove(11);
    ret = s.get(11);
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

    // random insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(randkey[i], buf[randkey[i]]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist random insert to empty list:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // drain the list
    for (uint64_t i = 0; i < loop; ++i){
        s.pop();
    }
    void * ret = s.pop();
    EXPECT_EQ(ret, nullptr);

    // Seq insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(i, buf[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist sequencial insert to empty list:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // random insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(randkey[i], buf[randkey[i]]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist random insert to full list:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // Seq insert()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.insert(i, buf[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist sequencial insert to full list:"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // get(key)
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get(i);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get(key):"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // get()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.get();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist get():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

    // pop()
    start = Cycles::rdtscp();
    for (uint64_t i = 0; i < loop; ++i){
        s.pop();
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "Skiplist pop():"
    << Cycles::toNanoseconds(stop - start)/loop << " nano sec per call " << std::endl;

}

void multiThreadTest(SkiplistTest *t)
{
    int foo;
    srand((uint64_t)&foo >> 32);
    for (uint64_t i = 0; i < t->loop; ++i) {
        int op = rand() % 5;
        switch(op) {
        case 0: t->s.insert(i, t->buf[i]); break;
        case 1: t->s.remove(i); break;
        case 2: t->s.pop(); break;
        case 3: t->s.try_pop(i); break;
        case 4: t->s.get(i); break;
        default: break;
        }
    }
}

TEST_F(SkiplistTest, MtTest) {
    std::thread t1(multiThreadTest, this);
    std::thread t2(multiThreadTest, this);
    std::thread t3(multiThreadTest, this);

    t1.join();
    t2.join();
    t3.join();
}

}  // namespace RAMCloud
