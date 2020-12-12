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
#include "HashmapKVStore.h"
#include "DataLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

class DataLogTest : public ::testing::Test {
  public:
  DataLogTest() {};
  ~DataLogTest() { dlog->clear(); delete dlog; };

  HashmapKVStore kvStore;

  DataLog *dlog = new DataLog(0);;

  DISALLOW_COPY_AND_ASSIGN(DataLogTest);
};

TEST_F(DataLogTest, DataLogUnitTest)
{
    #define LOOP 1024*4
    uint64_t off[LOOP];

    // Verify initial value
    EXPECT_EQ(dlog->size(), (size_t)0);

    // Simple test
    uint64_t doff = dlog->add("test", 4);
    uint32_t dlen;
    char* data = (char*)dlog->getdata(doff, &dlen);

    EXPECT_EQ(dlen, (uint32_t)4);
    EXPECT_EQ(strncmp(data, "test", 4), 0);

    uint64_t doff_1 = dlog->add("test-1", 6);
    data = (char*)dlog->getdata(doff_1, &dlen);
    EXPECT_EQ(strncmp(data, "test-1", 6), 0);
    EXPECT_EQ(dlen, (uint32_t)6);

    uint64_t doff_2 = dlog->add("test-2", 6);
    data = (char*)dlog->getdata(doff_2, &dlen);
    EXPECT_EQ(strncmp(data, "test-2", 6), 0);
    EXPECT_EQ(dlen, (uint32_t)6);

    bool ret = dlog->trim(1);
    EXPECT_EQ(ret, false);
    
    ret = dlog->trim(doff_1);
    EXPECT_EQ(ret, true);

    // Verify old data after a trim
    data = (char*)dlog->getdata(doff, &dlen);
    EXPECT_EQ(data, (char *)NULL);

    data = (char*)dlog->getdata(doff_2, &dlen);
    EXPECT_EQ(strncmp(data, "test-2", 6), 0);
    EXPECT_EQ(dlen, (uint32_t)6);

    for (uint32_t idx = 0; idx < LOOP; idx++) {
        char dbuf[32];
        sprintf(dbuf, "test-data-%04d", idx);
        off[idx] = dlog->add(dbuf, strlen(dbuf));
    }

    for (uint32_t idx = 0; idx < LOOP; idx++) {
        char dbuf[32];
        sprintf(dbuf, "test-data-%04d", idx);
        uint32_t len;
        char *data = (char *)dlog->getdata(off[idx], &len);
        EXPECT_EQ(len, strlen(dbuf));
        EXPECT_EQ(strncmp(data, dbuf, strlen(dbuf)), 0);
    }
}

static void multiThreadTest(DataLogTest *t, int id)
{
    uint64_t off[LOOP];
    for(int ii = 0; ii < LOOP; ii++) {
        char dbuf[32];
        sprintf(dbuf, "mthread-data-%04d-%d", ii, id);
        off[ii] = t->dlog->add(dbuf, strlen(dbuf));
    }

    for (uint32_t ii = 0; ii < LOOP; ii++) {
        char dbuf[32];
        sprintf(dbuf, "mthread-data-%04d-%d", ii, id);
        uint32_t len;
        char *data = (char *)t->dlog->getdata(off[ii], &len);
        EXPECT_EQ(len, strlen(dbuf));
        EXPECT_EQ(strncmp(data, dbuf, strlen(dbuf)), 0);
    }
}

TEST_F(DataLogTest, DataLogMultiThreadTest) {
    std::thread t1(multiThreadTest, this, 0);
    std::thread t2(multiThreadTest, this, 1);
    std::thread t3(multiThreadTest, this, 2);

    t1.join();
    t2.join();
    t3.join();
}

}  // namespace RAMCloud
