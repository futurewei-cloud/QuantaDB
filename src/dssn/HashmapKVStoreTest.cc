/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "HashmapKVStore.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class HashmapKVTest : public ::testing::Test {
  public:
  HashmapKVTest() {};
  ~HashmapKVTest() {};

  HashmapKVStore KVStore;

  DISALLOW_COPY_AND_ASSIGN(HashmapKVTest);
};

TEST_F(HashmapKVTest, preput) {
    GTEST_COUT << "HashmapKVTest" << std::endl;
    int keySize = 32;
    KVLayout kvIn(keySize), *kvOut;
    snprintf((char *)kvIn.getKey(), keySize, "HashmapKVTest-kvIn-key1");
    kvOut = KVStore.preput(kvIn);
    delete kvOut;
}

TEST_F(HashmapKVTest, putNew) {
    int keySize = 32;
    KVLayout kv(keySize);
    snprintf((char *)kv.k.key.get(), keySize - 1, "HashmapKVTest-key-1");
    bool ret = KVStore.putNew(&kv, 0, 0); 
    EXPECT_EQ(ret, true);
    ret = KVStore.putNew(&kv, 0, 0);
    EXPECT_EQ(ret, false);
}

TEST_F(HashmapKVTest, putNewBench) {
    int keySize = 32;
    KVLayout kv(keySize);
    int loop = 1024;
    uint64_t start, stop;
    //
    start = __rdtsc();
    for(int idx = 0; idx < loop; idx++) {
        snprintf((char *)kv.k.key.get(), keySize - 1, "HashmapKVTest-key-%04d", idx);
        KVLayout * ret = NULL;
        EXPECT_EQ(ret, (KVLayout*)NULL);
    }
    stop = __rdtsc();
    uint32_t overhead = Cycles::toNanoseconds(stop - start)/loop;
    //
    start = __rdtsc();
    for(int idx = 0; idx < loop; idx++) {
        snprintf((char *)kv.k.key.get(), keySize - 1, "HashmapKVTest-key-%04d", idx);
        bool ret = KVStore.putNew(&kv, 0, 0); 
        EXPECT_EQ(ret, true);
    }
    stop = __rdtsc();
    uint32_t nsec_per = Cycles::toNanoseconds(stop - start)/loop - overhead;
    GTEST_COUT << "HashmapKVTest putNew: " << nsec_per << " nano sec per call " << std::endl;

    start = __rdtsc();
    for(int idx = 0; idx < loop; idx++) {
        snprintf((char *)kv.k.key.get(), keySize - 1, "HashmapKVTest-key-%04d", idx);
        KVLayout * ret = KVStore.fetch(kv.k);
        EXPECT_NE(ret, (KVLayout*)NULL);
    }
    stop = __rdtsc();
    nsec_per = Cycles::toNanoseconds(stop - start)/loop - overhead;
    GTEST_COUT << "HashmapKVTest fetch: " << nsec_per << " nano sec per call " << std::endl;
}

TEST_F(HashmapKVTest, put) {
    int keySize = 32;
    KVLayout kvIn(keySize);
    snprintf((char *)kvIn.getKey(), keySize, "HashmapKVTest-kvIn-key1");
    int vallen = 256;
    uint8_t * val = (uint8_t *)malloc(256); 
    bool ret = KVStore.put(&kvIn, 0, 0, val, vallen);
    EXPECT_EQ(ret, true);
}

TEST_F(HashmapKVTest, fetch) {
    int keySize = 32;
    KVLayout kvIn(keySize), *kvOut;
    snprintf((char *)kvIn.getKey(), keySize, "HashmapKVTest-kvIn-key1");

    kvOut = KVStore.fetch(kvIn.k);
    EXPECT_EQ(kvOut, (KVLayout*)0);

    bool ret = KVStore.putNew(&kvIn, 0, 0); 
    EXPECT_EQ(ret, true);

    kvOut = KVStore.fetch(kvIn.k);
    EXPECT_EQ(kvOut, &kvIn);

    GTEST_COUT << "HashmapKVwStore fetch passed!" << std::endl;
}

}  // namespace RAMCloud
