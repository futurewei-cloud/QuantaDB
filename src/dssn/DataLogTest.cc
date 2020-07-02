/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "HashmapKVStore.h"
#include "DataLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class DataLogTest : public ::testing::Test {
  public:
  DataLogTest() {};
  ~DataLogTest() { delete dlog; };

  HashmapKVStore kvStore;

  DataLog *dlog = new DataLog(999);;

  DISALLOW_COPY_AND_ASSIGN(DataLogTest);
};

TEST_F(DataLogTest, DataLogUnitTest)
{
    #define LOOP 1024*4
    uint64_t off[LOOP];

    dlog->trim(0);

    EXPECT_EQ(dlog->size(), (size_t)0);

    uint64_t doff = dlog->add("test", 4);
    uint32_t dlen;
    char* data = (char*)dlog->getdata(doff, &dlen);

    EXPECT_EQ(dlen, (uint32_t)4);
    EXPECT_EQ(strncmp(data, "test", 4), 0);

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
