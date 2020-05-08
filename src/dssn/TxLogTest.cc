/*
 * Copyright (c) 2020  Futurewei Technologies, Inc.
 */
#include "TestUtil.h"
#include "TxLog.h"
#include "Cycles.h"

#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class TxLogTest : public ::testing::Test {
  public:
  TxLogTest() {};
  ~TxLogTest() {};

  TxLog txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogTest);
};

class TxLogRecoveryTest : public ::testing::Test {
  public:
  TxLogRecoveryTest() { txlog = new TxLog(true); };
  ~TxLogRecoveryTest() {};

  TxLog *txlog;

  DISALLOW_COPY_AND_ASSIGN(TxLogRecoveryTest);
};

TEST_F(TxLogTest, TxLogUnitTest)
{
    EXPECT_EQ(txlog.size(), (size_t)0);

    for (uint64_t idx = 0; idx < 100; idx++) {
        TxEntry tx(10,10);
        tx.setCTS(idx);
        tx.setPStamp(idx);
        tx.setSStamp(idx);
        tx.setTxState(((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT); 
        txlog.add(&tx);
    }

    // getTxState
    for (uint64_t idx = 0; idx < 100; idx++) {
        uint32_t tx_state = ((idx % 2) == 0)? TxEntry::TX_PENDING : TxEntry::TX_COMMIT;
        EXPECT_EQ(txlog.getTxState(idx), tx_state);
    }

    uint64_t idOut;
    DSSNMeta meta;
    std::set<uint64_t> peerSet;
    boost::scoped_array<KVLayout*> writeSet;
    bool ret = txlog.getFirstPendingTx(idOut, meta, peerSet, writeSet);

    EXPECT_EQ(ret, true);
    EXPECT_EQ(meta.pStamp, (uint64_t)0);

    uint64_t idIn = idOut;
    uint32_t ctr = 0;
    while (txlog.getNextPendingTx(idIn, idOut, meta, peerSet, writeSet)) {
        idIn = idOut;
        EXPECT_TRUE((meta.pStamp % 2) == 0);
        ctr++;
    }

    EXPECT_EQ(ctr, (uint32_t)100/2 -1);

}

TEST_F(TxLogRecoveryTest, TxLogUnitTest)
{
    EXPECT_GT(txlog->size(), (size_t)0);

    // getTxState
    for (uint64_t idx = 0; idx < 100; idx++) {
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

    EXPECT_EQ(ctr, (uint32_t)100/2 -1);
}

}  // namespace RAMCloud
