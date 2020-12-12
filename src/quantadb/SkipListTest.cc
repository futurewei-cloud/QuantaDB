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
#include "SkipList.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

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

void threadSafeTest(SkiplistTest *t)
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

TEST_F(SkiplistTest, MtSafeTest) {
    std::thread t1(threadSafeTest, this);
    std::thread t2(threadSafeTest, this);
    std::thread t3(threadSafeTest, this);

    t1.join();
    t2.join();
    t3.join();
}

void mtInsertTest(SkiplistTest *t, uint32_t start, uint32_t len)
{
    assert(start+len < t->loop);
    for (uint32_t i = start; i < start+len; ++i) {
        t->s.insert(i, t->buf[i]);
    }
}

void mtPopTest(SkiplistTest *t, uint32_t len)
{
    for (uint32_t i = 0; i < len; ++i) {
        t->s.pop();
    }
}

void mtRemoveTest(SkiplistTest *t, uint32_t start, uint32_t len)
{
    assert(start+len < t->loop);
    for (uint32_t i = start; i < start+len; ++i) {
        t->s.remove(i);
    }
}

void emptySkipList(SkipList<uint64_t> *s)
{
    while (s->ctr > 0)
        s->pop();

    assert(s->pop() == NULL);
}

TEST_F(SkiplistTest, MtCorrectnessTest) {
    emptySkipList(&s);

    std::thread t1(mtInsertTest, this, 0, 100);
    std::thread t2(mtInsertTest, this, 101, 100);
    std::thread t3(mtInsertTest, this, 201, 100);

    t1.join();
    t2.join();
    t3.join();

    EXPECT_EQ(s.ctr, (uint32_t)300);

    std::thread t4(mtPopTest, this, 50);
    std::thread t5(mtPopTest, this, 50);
    std::thread t6(mtPopTest, this, 50);

    t4.join();
    t5.join();
    t6.join();

    EXPECT_EQ(s.ctr, (uint32_t)150);

    std::thread t7(mtRemoveTest, this, 0, 100);
    std::thread t8(mtRemoveTest, this, 101, 100);
    std::thread t9(mtRemoveTest, this, 201, 100);

    t7.join();
    t8.join();
    t9.join();

    EXPECT_EQ(s.ctr, (uint32_t)0);
}

}  // namespace RAMCloud
