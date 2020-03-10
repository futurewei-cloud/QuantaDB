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

  HashmapKV KVStore;

  DISALLOW_COPY_AND_ASSIGN(HashmapKVTest);
};

TEST_F(HashmapKVTest, getCTS) {
    GTEST_COUT << "HashmapKVTest" << std::endl;
    int keySize = 32;
    KVLayout kv(keySize);
    snprintf((char *)kv.k.key.get(), keySize - 1, "HashmapKVTest-key-1");
    bool ret = KVStore.putNew(&kv, 0, 0); 
    EXPECT_EQ(ret, true);
}

}  // namespace RAMCloud
