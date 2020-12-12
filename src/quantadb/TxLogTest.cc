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
#include "TxLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace QDB;

class TxLogTest : public ::testing::Test {
  public:
  #define RWSetSize   10
  KVLayout * writeKV[RWSetSize], * readKV[RWSetSize];

  TxLogTest()
  {
    txlog = new TxLog(false, "unittest");

    // Init readSet and writeSet
    for (int idx = 0; idx < RWSetSize; idx++) {
        writeKV[idx] = new KVLayout(32);
        readKV[idx]  = new KVLayout(32);
        snprintf((char *)writeKV[idx]->getKey().key.get(), 32, "TxLogUnitTest-wkey%d", idx);
        snprintf((char *) readKV[idx]->getKey().key.get(), 32, "TxLogUnitTest-rkey%d", idx);
    }

  };

  ~TxLogTest() {
    for (int idx = 0; idx < RWSetSize; idx++) {
        delete writeKV[idx];
        delete readKV[idx];
    }
    delete txlog;
  };

  HashmapKVStore kvStore;
  TxLog *txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogTest);
};

class TxLogRecoveryTest : public ::testing::Test {
  public:
  TxLogRecoveryTest()  { txlog = new TxLog(true, "unittest"); };
  ~TxLogRecoveryTest() { delete txlog; };

  TxLog *txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogRecoveryTest);
};

#define NUM_ENTRY   1000
TEST_F(TxLogTest, TxLogUnitTest)
{
    uint64_t idOut;
    DSSNMeta meta;
    std::set<uint64_t> peerSet;
    boost::scoped_array<KVLayout*> writeSet;

    EXPECT_EQ(txlog->size(), (size_t)0);

    bool ret = txlog->getFirstPendingTx(idOut, meta, peerSet, writeSet);

    EXPECT_EQ(ret, false);

    for (uint64_t idx = 0; idx < NUM_ENTRY; idx++) {
        TxEntry tx(10,10);
        tx.setCTS(idx);
        tx.setPStamp(idx);
        tx.setSStamp(idx);
        tx.setTxState(((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT); 
        tx.insertPeerSet(idx);

        KVLayout *kvR, *kvW;
        for (int kvidx = 0; kvidx < RWSetSize; kvidx++) {
            kvR = kvStore.preput(*readKV[kvidx]);
            kvW = kvStore.preput(*writeKV[kvidx]);
            tx.insertWriteSet(kvW, kvidx);
            tx.insertReadSet (kvR, kvidx);
        }

        txlog->add(&tx);
    }

    // getTxState
    for (uint64_t idx = 0; idx < NUM_ENTRY; idx++) {
        uint32_t tx_state = ((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT;
        if (txlog->getTxState(idx) != tx_state)
            GTEST_COUT << "idx=" << idx << std::endl;
        EXPECT_EQ(txlog->getTxState(idx), tx_state);
    }

    // getTxInfo
    for (uint64_t idx = 0; idx < NUM_ENTRY; idx++) {
        uint32_t tx_state = ((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT;
        uint32_t txState;
        uint64_t pStamp, sStamp;
        bool ret = txlog->getTxInfo(idx, txState, pStamp, sStamp);
        if (!ret)
            continue;
        EXPECT_EQ(ret, true);
        EXPECT_EQ(tx_state, txState);
        EXPECT_EQ(pStamp, idx);
        EXPECT_EQ(sStamp, idx);
    }

    ret = txlog->getFirstPendingTx(idOut, meta, peerSet, writeSet);

    EXPECT_EQ(ret, true);
    EXPECT_EQ(meta.pStamp, (uint64_t)0);

    uint64_t idIn = idOut;
    uint32_t ctr = 0;
    while (txlog->getNextPendingTx(idIn, idOut, meta, peerSet, writeSet)) {
        idIn = idOut;
        EXPECT_TRUE((meta.pStamp % 2) == 0);
        ctr++;
    }

    EXPECT_EQ(ctr, (uint32_t)NUM_ENTRY/2 -1);

}

TEST_F(TxLogRecoveryTest, TxLogRecoveryTest)
{
    EXPECT_GT(txlog->size(), (size_t)0);

    // getTxState
    for (__uint128_t idx = 0; idx < NUM_ENTRY; idx++) {
        uint32_t tx_state = ((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT;
        EXPECT_EQ(txlog->getTxState(idx), tx_state);
    }

    //
    uint64_t idOut;
    DSSNMeta meta;
    std::set<uint64_t> peerSet;
    boost::scoped_array<KVLayout*> writeSet;
    bool ret = txlog->getFirstPendingTx(idOut, meta, peerSet, writeSet);

    EXPECT_EQ(ret, true);
    //EXPECT_EQ(meta.pStamp, (uint64_t)0);

    uint64_t idIn = idOut;
    uint32_t ctr = 0;
    while (txlog->getNextPendingTx(idIn, idOut, meta, peerSet, writeSet)) {
        idIn = idOut;
        EXPECT_TRUE((meta.pStamp % 2) == 0);
        ctr++;
    }

    // EXPECT_EQ(ctr, (uint32_t)NUM_ENTRY/2 -1);
}

void writeToLog(TxLogTest *c, int sid)
{
    TxEntry tx(RWSetSize, RWSetSize);
    for (__uint128_t idx = 0; idx < NUM_ENTRY; idx++) {
        __uint128_t stamp = idx + (NUM_ENTRY * sid);
        tx.setCTS(stamp);
        tx.setPStamp(stamp);
        tx.setSStamp(stamp);
        tx.setTxState(((stamp % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT); 
        tx.insertPeerSet(stamp);

        KVLayout *kvR, *kvW;
        for (int kvidx = 0; kvidx < RWSetSize; kvidx++) {
            kvR = c->kvStore.preput(*c->readKV[kvidx]);
            kvW = c->kvStore.preput(*c->writeKV[kvidx]);
            tx.insertWriteSet(kvW, kvidx);
            tx.insertReadSet (kvR, kvidx);
        }

        c->txlog->add(&tx);
    }
}

void readForwardFromLog(TxLogTest *c, int *run_run)
{
    while(*run_run)
    {
        uint64_t idIn = 0, idOut;
        DSSNMeta meta;
        std::set<uint64_t> peerSet;
        boost::scoped_array<KVLayout*> writeSet;
        while (c->txlog->getNextPendingTx(idIn, idOut, meta, peerSet, writeSet)) {
            idIn = idOut;
            EXPECT_TRUE((meta.pStamp % 2) == 0);
        }
    }
}

void readBackwardFromLog(TxLogTest *c, int *run_run)
{
    while(*run_run)
    {
        uint32_t txState;
        uint64_t pStamp, sStamp;
        c->txlog->getTxInfo(100, txState, pStamp, sStamp);
        c->txlog->getTxState(100);
    }
}


TEST_F(TxLogTest, TxLogMtRwTest)
{
    int run_run = 1;
    std::thread tR1(readBackwardFromLog, this, &run_run);
    std::thread tR2(readForwardFromLog, this, &run_run);
    std::thread tW1(writeToLog, this, 0);
    std::thread tW2(writeToLog, this, 1);

    tW1.join();
    tW2.join();
    run_run = 0;
    tR1.join();
    tR2.join();
}


}  // namespace RAMCloud
