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
#include "Cycles.h"
#include "MemStreamIo.h"
#include "KVStore.h"
#include "TxEntry.h"
#include "HashmapKVStore.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

class MemStreamIoTest : public ::testing::Test {
  public:
  MemStreamIoTest() {};
  ~MemStreamIoTest() {};

  uint8_t buf[1024*1024];

  HashmapKVStore kvStore;   // needto borrow the preput()

  DISALLOW_COPY_AND_ASSIGN(MemStreamIoTest);
};

TEST_F(MemStreamIoTest, MemStreamIoUnitTest)
{
    // KLayout test
    KLayout k1(30), k2(30);

    const char *keystr = (const char *)"MemStreamIoTestKey";
    uint8_t *valstr = const_cast<uint8_t *>((const uint8_t*)"MemStreamIoTestValue");

    memcpy(k1.key.get(), keystr, strlen(keystr));
    outMemStream out(buf, sizeof(buf));
    k1.serialize(out);

    EXPECT_EQ(out.dsize(), k1.serializeSize());

    inMemStream in(buf, out.dsize());
    k2.deSerialize( in );
    EXPECT_EQ(k1, k2);

    // KVLayout
    KVLayout kv1(30), kv2(30);
    memcpy(kv1.k.key.get(), keystr, strlen(keystr));
    kv1.v.valuePtr = valstr;
    kv1.v.valueLength = strlen((char *)valstr);
    kv1.v.meta.pStamp = 0xF0F0F0F0;
    kv1.v.isTombstone = true;

    outMemStream out1(buf, sizeof(buf));
    kv1.serialize( out1 );

    EXPECT_EQ(out1.dsize(), kv1.serializeSize());

    inMemStream in1(buf, out1.dsize());
    kv2.deSerialize( in1 );
    EXPECT_EQ(kv1.k, kv2.k);
    EXPECT_EQ(kv1.v.valueLength, kv2.v.valueLength);
    EXPECT_EQ(kv1.v.meta.pStamp, kv2.v.meta.pStamp);
    EXPECT_EQ(kv1.v.isTombstone, kv2.v.isTombstone);
    EXPECT_EQ(strncmp((const char*)kv1.v.valuePtr, (const char*)kv2.v.valuePtr, kv1.v.valueLength), 0);

    // TxEntry
    #define WRITESET_SIZE 7
    TxEntry tx1(1, WRITESET_SIZE), tx2(1, 1);

    char kbuf[30];
    // KVLayout **writeSet = tx1.getWriteSet().get();
    for (uint32_t idx = 0; idx < WRITESET_SIZE; idx++) {
        KVLayout kvbuf(30);
        sprintf(kbuf, "MemStreamIoTestKey-%d", idx);
        memcpy(kvbuf.k.key.get(), kbuf, strlen(kbuf));
        kvbuf.v.valuePtr = valstr;
        kvbuf.v.valueLength = strlen((char *)valstr);

        KVLayout *kv = kvStore.preput(kvbuf);

        tx1.insertWriteSet(kv, idx);
    }

    EXPECT_EQ(tx1.getWriteSetSize(), (uint32_t)WRITESET_SIZE);

    tx1.txState = TxEntry::TX_PENDING;
    tx1.commitIntentState = TxEntry::TX_CI_SCHEDULED;

    outMemStream out2(buf, sizeof(buf));
    tx1.serialize( out2 );

    EXPECT_EQ(out2.dsize(), tx1.serializeSize());

    inMemStream in2(buf, out2.dsize());
    tx2.deSerialize ( in2 );

    EXPECT_EQ(tx1.getTxState(), tx2.getTxState());
    EXPECT_EQ(tx1.getTxCIState(), tx2.getTxCIState());
    EXPECT_EQ(tx1.getWriteSetSize(), tx2.getWriteSetSize());


    KVLayout **writeSet1 = tx1.getWriteSet().get();
    KVLayout **writeSet2 = tx2.getWriteSet().get();

    for(uint32_t idx = 0; idx < tx2.getWriteSetSize(); idx++) {
        KVLayout *kv1 = writeSet1[idx],
                 *kv2 = writeSet2[idx];
        EXPECT_EQ(kv1->k, kv2->k);
        EXPECT_EQ(kv1->v.valueLength, kv2->v.valueLength);
        EXPECT_EQ(kv1->v.meta.pStamp, kv2->v.meta.pStamp);
        EXPECT_EQ(kv1->v.isTombstone, kv2->v.isTombstone);
        EXPECT_EQ(strncmp((const char*)kv1->v.valuePtr, (const char*)kv2->v.valuePtr, kv1->v.valueLength), 0);
    }

    EXPECT_EQ(tx1.getPeerSet(), tx2.getPeerSet());
}

}  // namespace RAMCloud
