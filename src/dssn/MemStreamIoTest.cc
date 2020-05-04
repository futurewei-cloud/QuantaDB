/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "Cycles.h"
#include "MemStreamIo.h"
#include "KVStore.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class MemStreamIoTest : public ::testing::Test {
  public:
  MemStreamIoTest() {};
  ~MemStreamIoTest() {};

  uint8_t buf[1024*1024];


  DISALLOW_COPY_AND_ASSIGN(MemStreamIoTest);
};

TEST_F(MemStreamIoTest, MemStreamIoUnitTest)
{
    KLayout k1(30), k2(30);
    GTEST_COUT << "MemStreamIoTest" << std::endl;

    const char *keystr = (const char *)"MemStreamIoTestKey";
    uint8_t *valstr = const_cast<uint8_t *>((const uint8_t*)"MemStreamIoTestValue");

    memcpy(k1.key.get(), keystr, strlen(keystr));
    outMemStream out(buf, sizeof(buf));
    k1.serialize(out);

    inMemStream in(buf, out.dsize());
    k2.deSerialize( in );
    EXPECT_EQ(k1, k2);

    KVLayout kv1(30), kv2(30);
    memcpy(kv1.k.key.get(), keystr, strlen(keystr));
    kv1.v.valuePtr = valstr;
    kv1.v.valueLength = strlen((char *)valstr);
    kv1.v.meta.pStamp = 0xF0F0F0F0;
    kv1.v.isTombstone = true;

    outMemStream out1(buf, sizeof(buf));
    kv1.serialize( out1 );

    inMemStream in1(buf, out1.dsize());
    kv2.deSerialize( in1 );
    EXPECT_EQ(kv1.k, kv2.k);
    EXPECT_EQ(kv1.v.valueLength, kv2.v.valueLength);
    EXPECT_EQ(kv1.v.meta.pStamp, kv2.v.meta.pStamp);
    EXPECT_EQ(kv1.v.isTombstone, kv2.v.isTombstone);
    EXPECT_EQ(strncmp((const char*)kv1.v.valuePtr, (const char*)kv2.v.valuePtr, kv1.v.valueLength), 0);
}

}  // namespace RAMCloud
