/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "HashmapKVStore.h"
#include "TxLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class TxLogTest : public ::testing::Test {
  public:
  TxLogTest() { txlog = new TxLog(false, "unittest"); };
  ~TxLogTest() {};

  HashmapKVStore kvStore;

  TxLog *txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogTest);
};

class TxLogRecoveryTest : public ::testing::Test {
  public:
  TxLogRecoveryTest() { txlog = new TxLog(true, "unittest"); };
  ~TxLogRecoveryTest() {};

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

    #define RWSetSize   10
    KVLayout * writeKV[RWSetSize], * readKV[RWSetSize];

    for (int idx = 0; idx < RWSetSize; idx++) {
        writeKV[idx] = new KVLayout(32);
        readKV[idx]  = new KVLayout(32);
        snprintf((char *)writeKV[idx]->getKey().key.get(), 32, "TxLogUnitTest-wkey%d", idx);
        snprintf((char *) readKV[idx]->getKey().key.get(), 32, "TxLogUnitTest-rkey%d", idx);
    }

    for (uint64_t idx = 0; idx < NUM_ENTRY; idx++) {
        TxEntry tx(10,10);
        tx.setCTS(idx);
        tx.setPStamp(idx);
        tx.setSStamp(idx);
        tx.setTxState(((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT); 
        tx.insertPeerSet(idx);

        for (int kvidx = 0; kvidx < RWSetSize; kvidx++) {
            KVLayout *kvR = kvStore.preput(*readKV[kvidx]);
            KVLayout *kvW = kvStore.preput(*writeKV[kvidx]);
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

TEST_F(TxLogRecoveryTest, TxLogUnitTest)
{
    EXPECT_GT(txlog->size(), (size_t)0);

    // getTxState
    for (uint64_t idx = 0; idx < NUM_ENTRY; idx++) {
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

}  // namespace RAMCloud
