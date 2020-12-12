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
#include "MockCluster.h"
#include "LeaseCommon.h"
#include "Validator.h"
#include "Tub.h"
#include "MultiWrite.h"
#include "Cycles.h"

#include <ostream>
#include <string>
#define GTEST_COUT  std::cerr << std::scientific << "[ INFO ] "

namespace QDB {

using namespace RAMCloud;

class ValidatorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ClusterClock clusterClock;
    HashmapKVStore kvStore;
    QDB::Validator validator;
    TxEntry *txEntry[1000000];
    uint8_t dataBlob[512];

    ValidatorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , clusterClock()
		, validator(kvStore, NULL, true)
    {
    	memset(txEntry, 0, sizeof(txEntry));
    }

    DISALLOW_COPY_AND_ASSIGN(ValidatorTest);

    void fillTxEntry(int noEntries, int noKeys = 1, int noOfPeers = 0) {
    	//prepare batches of 10 CIs of the same keys
        static __uint128_t ctsBase = (__uint128_t)10 << 64; //started with 10ns
    	uint32_t batchSize = 10;
    	uint32_t keySize = 32;
        for (int i = 0; i < noEntries; i++) {
        	uint32_t rr = 0, ww = 0;
        	txEntry[i] = new TxEntry(4 * noKeys / 5, (noKeys + 4) / 5);
        	txEntry[i]->setCTS(ctsBase);
        	ctsBase += (__uint128_t)1 << 64; //increased by 1ns
        	for (int j = 0; j < noKeys; j++) {
                KVLayout kv(keySize);
                snprintf((char *)kv.k.key.get(),
                		keySize, "%d$%d0123456789abcdef0123456789abcdef", i % batchSize, j);
                kv.k.keyLength = keySize;
                kv.v.valuePtr = (uint8_t *)dataBlob;
                kv.v.valueLength = sizeof(dataBlob);
                KVLayout *kvOut = validator.kvStore.preput(kv);
        		if (j % 5 == 0)
        			txEntry[i]->insertWriteSet(kvOut, rr++);
        		else
        			txEntry[i]->insertReadSet(kvOut, ww++);
        	}

        	for (int peerId = 0; peerId < noOfPeers; peerId++) {
        		//cross-shard transaction
        		txEntry[i]->insertPeerSet(peerId);
        	}
        }
    }

    void fillTxEntryPeers(TxEntry *txEntry) {
        uint64_t myPStamp, mySStamp;
        uint32_t myTxState;
    	validator.peerInfo.add(txEntry->getCTS(), txEntry, &validator);
    	for (uint64_t peerId = 0; peerId <= txEntry->getPeerSet().size(); peerId++) {
    		validator.receiveSSNInfo(peerId, txEntry->getCTS(), 0, 0xfffffff, txEntry->getTxState(),
    		                 myPStamp, mySStamp, myTxState);
    	}

    }

    //should be called to match fillTxEntry() because
    //conclude() is muted from freeing txEntry[] during uni test
    void freeTxEntry(int noEntries) {
        for (int i = 0; i < noEntries; i++) {
        	delete txEntry[i];
        	txEntry[i] = 0;
        }
    }

    void printTxEntry(int noEntries) {
        for (int i = 0; i < noEntries; i++) {
        	if (txEntry[i] == 0)
        		break;
        	for (uint32_t j = 0; j < txEntry[i]->getReadSetSize(); j++) {
        		std::string str((char *)txEntry[i]->readSet[j]->k.key.get());
        		GTEST_COUT << "read key: " << str  << std::endl;
        	}
        	for (uint32_t j = 0; j < txEntry[i]->getWriteSetSize(); j++) {
        		std::string str((char *)txEntry[i]->writeSet[j]->k.key.get());
        	    GTEST_COUT << "write key: " << str  << std::endl;
        	}
        }
    }

    void printTxEntryCommits(int noEntries) {
    	int count = 0;
    	for (int i = 0; i < noEntries; i++) {
    		if (txEntry[i]->txState == TxEntry::TX_COMMIT)
    			count++;
    	}
    	GTEST_COUT << "Total commits: " << count << std::endl;
    }
};

TEST_F(ValidatorTest, BATKVStorePutGet) {
    fillTxEntry(1);

    KVLayout *kv;
    for (uint32_t i = 0; i < txEntry[0]->getWriteSetSize(); i++) {
        kv = validator.kvStore.fetch(txEntry[0]->getWriteSet()[i]->k);
        ASSERT_TRUE(NULL==kv);
        //EXPECT_EQ(0, kv->v.valuePtr);
        //EXPECT_EQ(0, (int)kv->v.valueLength);

        validator.kvStore.putNew(txEntry[0]->getWriteSet()[i], 0, 0);
        kv = validator.kvStore.fetch(txEntry[0]->getWriteSet()[i]->k);
        ASSERT_TRUE(NULL!=kv);
        EXPECT_NE(dataBlob, kv->v.valuePtr);
        EXPECT_EQ(sizeof(dataBlob), kv->v.valueLength);
        EXPECT_EQ(0, std::memcmp(dataBlob, kv->v.valuePtr, kv->v.valueLength));
    }

    freeTxEntry(1);
}

TEST_F(ValidatorTest, BATKVStorePutPerf) {
    uint64_t start, stop;

    fillTxEntry(1,1000000);

    uint32_t size = txEntry[0]->getWriteSetSize();
    auto& writeSet = txEntry[0]->getWriteSet();
    start = Cycles::rdtscp();
    for (uint32_t i = 0; i < size; i++) {
        validator.kvStore.putNew(writeSet[i], 0, 0);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "write (" << size << " keys): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per write: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
    //printTxEntry(1);

    freeTxEntry(1);
}

TEST_F(ValidatorTest, BATKVStorePutGetMulti) {
    fillTxEntry(5, 10);
    KVLayout *kv;
    for (uint32_t i = 0; i < txEntry[0]->getWriteSetSize(); i++) {
        validator.kvStore.putNew(txEntry[0]->getWriteSet()[i], 0, 0);
        kv = validator.kvStore.fetch(txEntry[0]->getWriteSet()[i]->k);
        ASSERT_TRUE(NULL!=kv);
        EXPECT_EQ(sizeof(dataBlob), kv->v.valueLength);
        EXPECT_EQ(0, std::memcmp(dataBlob, kv->v.valuePtr, kv->v.valueLength));
    }
    freeTxEntry(5);
}

TEST_F(ValidatorTest, BATValidateLocalTx) {
    // this tests the correctness of local tx validation

    KVLayout *kv;

    fillTxEntry(1);
    KLayout k(txEntry[0]->getWriteSet()[0]->k.keyLength);
    std::memcpy(k.key.get(), txEntry[0]->getWriteSet()[0]->k.key.get(), k.keyLength);

    validator.localTxQueue.add(txEntry[0]);
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry[0]->txState); //COMMIT
    kv = validator.kvStore.fetch(k);
    ASSERT_TRUE(NULL!=kv);
    EXPECT_EQ(sizeof(dataBlob), kv->v.valueLength);
    EXPECT_EQ(0, std::memcmp(dataBlob, kv->v.valuePtr, kv->v.valueLength));

    freeTxEntry(1);

    fillTxEntry(1, 4); //one write key, three read keys

    validator.localTxQueue.add(txEntry[0]);
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry[0]->txState); //COMMIT
    kv = validator.kvStore.fetch(k);
    ASSERT_TRUE(NULL!=kv);
    EXPECT_EQ(sizeof(dataBlob), kv->v.valueLength);
    EXPECT_EQ(0, std::memcmp(dataBlob, kv->v.valuePtr, kv->v.valueLength));

    freeTxEntry(1);
}

TEST_F(ValidatorTest, BATValidateLocalTxPerf) {
	// this tests performance of local tx validation

    int size = 10000;//(int)(sizeof(txEntry) / sizeof(TxEntry *));
    int count = 0;
    uint64_t start, stop;

    fillTxEntry(size, 10);

    GTEST_COUT << "WriteSet size " << txEntry[0]->getWriteSetSize() << std::endl;
    GTEST_COUT << "ReadSet size " << txEntry[0]->getReadSetSize() << std::endl;

    count = 0;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	if (validator.localTxQueue.add(txEntry[i])) count++;
    }
    //validator.localTxQueue.schedule(true);
    stop = Cycles::rdtscp();
    GTEST_COUT << "localTxQueue.add(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
    EXPECT_EQ(size, count);

    //time pop()
    count = 0;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	if (validator.localTxQueue.pop(tmp)) count++;
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "localTxQueue.pop(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
    EXPECT_EQ(size, count);

    //time blocks()
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	if (validator.activeTxSet.blocks(txEntry[i])) {
    		EXPECT_EQ(0, 1);
    	}
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "activeTxSet.blocks(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    //time validate()
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.validateLocalTx(*txEntry[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "validateLocalTx(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    // time conclude()
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.conclude(txEntry[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "conclude(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    printTxEntryCommits(size);

    freeTxEntry(size);
}

TEST_F(ValidatorTest, BATValidateLocalTxPerf2) {
    int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    uint64_t start, stop;

    fillTxEntry(size, 2);

    //printTxEntry(1);
    GTEST_COUT << "WriteSet size " << txEntry[0]->getWriteSetSize() << std::endl;
    GTEST_COUT << "ReadSet size " << txEntry[0]->getReadSetSize() << std::endl;

    //time all operations
    for (int i = 0; i < size; i++) {
    	validator.localTxQueue.add(txEntry[i]);
    }

    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	validator.localTxQueue.pop(tmp);
    	validator.activeTxSet.blocks(tmp);
    	validator.validateLocalTx(*tmp);
    	validator.conclude(tmp);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "pop,blocks,validate,conclude: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    printTxEntryCommits(size);

    freeTxEntry(size);
}

TEST_F(ValidatorTest, BATValidateLocalTxs) {
    int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    uint64_t start, stop;

    fillTxEntry(size, 20);

    GTEST_COUT << "WriteSet size " << txEntry[0]->getWriteSetSize() << std::endl;
    GTEST_COUT << "ReadSet size " << txEntry[0]->getReadSetSize() << std::endl;

    //time serialize()
    for (int i = 0; i < size; i++) {
    	validator.localTxQueue.add(txEntry[i]);
    }
    //validator.localTxQueue.schedule(true);
    start = Cycles::rdtscp();
    validator.serialize();
    stop = Cycles::rdtscp();
    GTEST_COUT << "Serialize local txs: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    printTxEntryCommits(size);

    freeTxEntry(size);
}

TEST_F(ValidatorTest, BATActiveTxSet) {

    fillTxEntry(1, 10, 2); //5 txs of 10 keys and 2 peers

    //start cross-validation but leave it unfinished
    validator.activeTxSet.add(txEntry[0]);
    bool ret = validator.activeTxSet.blocks(txEntry[0]);
    EXPECT_EQ(true, ret);
    ret = validator.activeTxSet.remove(txEntry[0]);
    EXPECT_EQ(true, ret);
    ret = validator.activeTxSet.blocks(txEntry[0]);
    EXPECT_EQ(false, ret);

    freeTxEntry(1);
}


TEST_F(ValidatorTest, BATPeerInfo) {

	fillTxEntry(5, 10, 2); //5 txs of 10 keys and 2 peers

	for (int ent = 0; ent < 5; ent++) {
		validator.peerInfo.add(txEntry[ent]->getCTS(), txEntry[ent], &validator);
        txEntry[ent]->setTxCIState(TxEntry::TX_CI_LISTENING);
	}

	for (int ent = 1; ent < 4; ent++) {
		for (uint64_t peer = 0; peer < 2; peer++) {
		    uint64_t myPStamp, mySStamp;
		    uint32_t myTxState;
			EXPECT_EQ(TxEntry::TX_PENDING, txEntry[ent]->getTxState());
			validator.receiveSSNInfo(peer, txEntry[ent]->getCTS(), 0, 0xfffffff, TxEntry::TX_PENDING,
			        myPStamp, mySStamp, myTxState);
		}
		EXPECT_NE(TxEntry::TX_PENDING, txEntry[ent]->getTxState());
	}
	EXPECT_EQ((uint32_t)5, validator.peerInfo.size());
	//validator.peerInfo.sweep(&validator);
	//EXPECT_EQ((uint32_t)2, validator.peerInfo.size());

	freeTxEntry(5);
}

TEST_F(ValidatorTest, BATPeerInfo2) {

    fillTxEntry(2, 10, 1);

    uint64_t myPStamp, mySStamp;
    uint32_t myTxState;

    validator.peerInfo.add(txEntry[0]->getCTS(), txEntry[0], &validator);
    TxEntry* txEnt = validator.receiveSSNInfo(0, txEntry[0]->getCTS(), 0, 0xfffffff, TxEntry::TX_PENDING,
            myPStamp, mySStamp, myTxState);
    EXPECT_EQ(txEntry[0], txEnt);

    txEnt = validator.receiveSSNInfo(0, txEntry[1]->getCTS(), 0, 0xfffffff, TxEntry::TX_PENDING,
            myPStamp, mySStamp, myTxState);
    bool ret = validator.peerInfo.add(txEntry[1]->getCTS(), txEntry[1], &validator);
    EXPECT_EQ(true, ret);

    freeTxEntry(2);
}

TEST_F(ValidatorTest, BATPeerInfoReceivedEarly) {
    uint64_t myPStamp, mySStamp;
    uint32_t myTxState;
    __uint128_t cts = (__uint128_t)123 << 64;
    TxEntry *txEntry = validator.receiveSSNInfo(1 /*peerId*/,
            cts,
            0, 0xfffffff, /*pstamp, sstamp*/
            TxEntry::TX_PENDING,
            myPStamp, mySStamp, myTxState);
    EXPECT_EQ(true, txEntry == NULL);
}

TEST_F(ValidatorTest, BATValidateDistributedTxs) {
    int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    size = 20;

    fillTxEntry(size, 20, 3); //3 participants

    //time all operations
    int threshold = 100;
    validator.distributedTxSet.setHotThreshold(threshold);
    for (int i = 0; i < size; i++) {
    	validator.distributedTxSet.add(txEntry[i]);
    }

    EXPECT_EQ(size, (int)validator.distributedTxSet.count());

    for (int i = 0; i < size; i += 10) {
    	validator.serialize();
    	for (int j = 0; j < 10; j++) {
    		if (i + j  < size) {
    	        txEntry[i+j]->setTxCIState(TxEntry::TX_CI_LISTENING);
    			fillTxEntryPeers(txEntry[i + j]);
    		}
    	}
    }

	EXPECT_EQ(0, (int)validator.distributedTxSet.count());

	freeTxEntry(size);
}

TEST_F(ValidatorTest, BATDistributedTxSetPerf) {
    //int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    int size = 500000;
    uint64_t start, stop;

    fillTxEntry(size, 20, 2);

    //time all operations
    int threshold = 100;
    validator.distributedTxSet.setHotThreshold(threshold);
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.distributedTxSet.add(txEntry[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "distributedTxSet.add (" << size << ") [" <<  validator.distributedTxSet.count()
    		<< " ok]:  Total cycles " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per add: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    GTEST_COUT << "independ: " << validator.distributedTxSet.independentQueueCount()
    		<< "; cold: " << validator.distributedTxSet.coldQueueCount()
			<< "; hot: " << validator.distributedTxSet.hotQueueCount()
			<< std::endl;

    EXPECT_EQ(10, (int)validator.distributedTxSet.independentQueueCount());
    EXPECT_EQ(threshold * 10, (int)validator.distributedTxSet.coldQueueCount());
    EXPECT_EQ(500000 - 10 - threshold * 10, (int)validator.distributedTxSet.hotQueueCount());
    EXPECT_EQ(validator.distributedTxSet.count(),
    		validator.distributedTxSet.independentQueueCount() +
			validator.distributedTxSet.coldQueueCount() +
			validator.distributedTxSet.hotQueueCount());

    TxEntry *txEntry;
    uint64_t total = 0;
    __uint128_t lastCTS = 0;
    uint32_t count = 0;
	start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	txEntry = validator.distributedTxSet.findReadyTx(validator.activeTxSet);
    	if (txEntry) {
    		EXPECT_LT(lastCTS, txEntry->getCTS());
    		lastCTS = txEntry->getCTS();
    		count++;
    	}
    }
	stop = Cycles::rdtscp();
	total += stop - start;

	EXPECT_EQ(0, (int)validator.distributedTxSet.count());

    GTEST_COUT << "findReadyTx (" << size << "[" << count << " ok]): Total cycles " << total << std::endl;
    GTEST_COUT << "Sec per try: " << (Cycles::toSeconds(total) / size)  << std::endl;

	freeTxEntry(size);
}

TEST_F(ValidatorTest, BATLateDistributedTxs) {
    int size = 2;

    fillTxEntry(size, 20, 3); //3 participants

    //schedule a younger tx first
    validator.insertTxEntry(txEntry[1]);
    EXPECT_EQ(TxEntry::TX_PENDING, txEntry[1]->getTxState());
    validator.testRun();

    //then schedule an older tx
    validator.insertTxEntry(txEntry[0]);
    EXPECT_EQ(TxEntry::TX_PENDING, txEntry[0]->getTxState());
    validator.testRun();

    //the older tx is aborted
    EXPECT_EQ(TxEntry::TX_OUTOFORDER, txEntry[0]->getTxState());

    //the younger tx taking too long to complete is put in ALERT state
    EXPECT_EQ(TxEntry::TX_ALERT, txEntry[1]->getTxState());

    freeTxEntry(size);
}
/*
TEST_F(ValidatorTest, BATRecover) {
    int size = 10;

    validator.txLog.trim(0);

    fillTxEntry(size, 20, 3); //3 participants
    for (int i = 0; i < size; i++) {
        validator.insertTxEntry(txEntry[i]);
        validator.testRun();
    }

    EXPECT_EQ(size, (int)validator.activeTxSet.addedTxCount);

    validator.recover();
    EXPECT_EQ(size, (int)validator.counters.recovers);

    validator.lastScheduledTxCTS = 0;
    for (int i = 0; i < size; i++) {
        validator.testRun();
    }
    EXPECT_EQ(size * 2, (int)validator.activeTxSet.addedTxCount);

    freeTxEntry(size);
}

/*
TEST_F(ValidatorTest, BATDependencyMatrix) {
	fillTxEntry(35, 20, 2); //35 txs of 20 keys and 2 peers

	//add to reorder queue with bad order to test proper reordering
	for (int ent = 34; ent >= 0; ent--) {
		validator.reorderQueue.insert(txEntry[ent]->getCTS(), txEntry[ent]);
	}

	//schedule 25 txs
	for (int i = 0; i < 25; i++) {
		validator.scheduleDistributedTxs();
	}
	EXPECT_EQ(25, (int)validator.blockedTxSet.size());
	EXPECT_EQ(25, (int)validator.blockedTxSet.count());

	//start cross validation without finishing
	validator.serialize(); //expect first 10 removed but waist has not advanced yet
	EXPECT_EQ(24, (int)validator.blockedTxSet.size());
	EXPECT_EQ(15, (int)validator.blockedTxSet.count());

	validator.scheduleDistributedTxs(); //add 1 and would advance waist
	validator.serialize(); //would advance head
	EXPECT_EQ(16, (int)validator.blockedTxSet.size());

	//finish validation for 5 txs
	for (int ent = 0; ent < 5; ent++)
		fillTxEntryPeers(txEntry[ent]); //note: this will cause assertion if those 5 have not been added to activeTxSet
	validator.scheduleDistributedTxs(); //add 1
	validator.serialize(); //expect 5 more removed
	EXPECT_EQ(12, (int)validator.blockedTxSet.size());

	validator.scheduleDistributedTxs(); //add 1
	validator.serialize();
	EXPECT_EQ(13, (int)validator.blockedTxSet.size());

	fillTxEntryPeers(txEntry[7]); //finish 7th
	validator.serialize(); //expect 17th removed
	fillTxEntryPeers(txEntry[17]); //finish 17th
	validator.serialize(); //expect 27th removed
	validator.scheduleDistributedTxs(); //insert 1 at 27th place
	EXPECT_EQ(13, (int)validator.blockedTxSet.size());

	freeTxEntry(35);
}

TEST_F(ValidatorTest, BATDependencyMatrixPerf) {
    //int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    int size = 64;
    uint64_t start, stop;

    fillTxEntry(size, 2);

    //time all operations
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.blockedTxSet.add(txEntry[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "blockedTxSet.add (" << validator.blockedTxSet.size() << "): Total cycles " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per add: " << (Cycles::toSeconds(stop - start) / validator.blockedTxSet.size())  << std::endl;

    validator.serialize(); //remove all overhead of successful scheduling

    start = Cycles::rdtscp();
    validator.blockedTxSet.findReadyTx(validator.activeTxSet);
    stop = Cycles::rdtscp();
    GTEST_COUT << "findReadyTx (" << validator.blockedTxSet.size() << "): Total cycles " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per try: " << (Cycles::toSeconds(stop - start))  << std::endl;

	freeTxEntry(size);
}*/

}  // namespace RAMCloud
