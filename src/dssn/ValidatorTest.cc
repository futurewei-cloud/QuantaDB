/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "MockCluster.h"
#include "LeaseCommon.h"
#include "ValidatorRPCHelper.h"
#include "Validator.h"
#include "Tub.h"
#include "MultiWrite.h"
#include "Cycles.h"

#include <ostream>
#include <string>
#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class ValidatorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ClusterClock clusterClock;
    HashmapKVStore kvStore;
    DSSN::Validator validator;
    TxEntry *txEntry[1000000];
    uint8_t dataBlob[512];
	ValidatorRPCHelper helper;

    ValidatorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , clusterClock()
		, validator(kvStore)
		, helper(validator)
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ValidatorTest);

    void fillTxEntry(int noEntries, int noKeys = 1, int noOfPeers = 0) {
    	//prepare batches of 10 CIs of the same keys
    	uint32_t batchSize = 10;
    	uint32_t keySize = 32;
        for (int i = 0; i < noEntries; i++) {
        	uint32_t rr = 0, ww = 0;
        	txEntry[i] = new TxEntry(4 * noKeys / 5, (noKeys + 4) / 5);
        	txEntry[i]->setCTS((i+1) * 10);
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
    	validator.peerInfo.add(txEntry);
    	for (uint64_t peerId = 0; peerId <= txEntry->getPeerSet().size(); peerId++) {
    		helper.updatePeerInfo(txEntry->getCTS(), peerId, 0, 0xfffffff);
    	}

    }

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
/*
TEST_F(ValidatorTest, BATKVStorePutGet) {
	fillTxEntry(1);

	for (uint32_t i = 0; i < txEntry[0]->getWriteSetSize(); i++) {
		uint8_t *valuePtr = 0;
		uint32_t valueLength;
		validator.kvStore.getValue(txEntry[0]->getWriteSet()[i]->k, valuePtr, valueLength);
		EXPECT_EQ(0, valuePtr);
		EXPECT_EQ(0, (int)valueLength);
		validator.kvStore.putNew(txEntry[0]->getWriteSet()[i], 0, 0);
		validator.kvStore.getValue(txEntry[0]->getWriteSet()[i]->k, valuePtr, valueLength);
	    EXPECT_NE(dataBlob, valuePtr);
	    EXPECT_EQ(sizeof(dataBlob), valueLength);
	    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));
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
	for (uint32_t i = 0; i < txEntry[0]->getWriteSetSize(); i++) {
		validator.kvStore.putNew(txEntry[0]->getWriteSet()[i], 0, 0);
		uint8_t *valuePtr = 0;
		uint32_t valueLength;
		validator.kvStore.getValue(txEntry[0]->getWriteSet()[i]->k, valuePtr, valueLength);
	    EXPECT_EQ(sizeof(dataBlob), valueLength);
	    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));
	}
	freeTxEntry(5);
}

TEST_F(ValidatorTest, BATValidateLocalTx) {
	// this tests the correctness of local tx validation

	uint8_t *valuePtr = 0;
	uint32_t valueLength;

    fillTxEntry(1);
    KLayout k(txEntry[0]->getWriteSet()[0]->k.keyLength);
    std::memcpy(k.key.get(), txEntry[0]->getWriteSet()[0]->k.key.get(), k.keyLength);

    validator.localTxQueue.push(txEntry[0]);
    validator.localTxQueue.schedule(true);
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry[0]->txState); //COMMIT
	validator.kvStore.getValue(k, valuePtr, valueLength);
    EXPECT_EQ(sizeof(dataBlob), valueLength);
    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));

    freeTxEntry(1);

    fillTxEntry(1, 4); //one write key, three read keys

    validator.localTxQueue.push(txEntry[0]);
    validator.localTxQueue.schedule(true);
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry[0]->txState); //COMMIT
	validator.kvStore.getValue(k, valuePtr, valueLength);
    EXPECT_EQ(sizeof(dataBlob), valueLength);
    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));

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
    	if (validator.localTxQueue.push(txEntry[i])) count++;
    }
    validator.localTxQueue.schedule(true);
    stop = Cycles::rdtscp();
    GTEST_COUT << "localTxQueue.push(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
    EXPECT_EQ(size, count);

    //time pop()
    count = 0;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	if (validator.localTxQueue.try_pop(tmp)) count++;
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "localTxQueue.try_pop(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
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
    	validator.conclude(*txEntry[i]);
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
    	validator.localTxQueue.push(txEntry[i]);
    }
    validator.localTxQueue.schedule(true);

    //uint64_t lastCTS = 1234;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	validator.localTxQueue.try_pop(tmp);
    	validator.activeTxSet.blocks(tmp);
    	//tmp->setCTS(++lastCTS);
    	validator.validateLocalTx(*tmp);
    	validator.conclude(*tmp);
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
    	validator.localTxQueue.push(txEntry[i]);
    }
    validator.localTxQueue.schedule(true);
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    start = Cycles::rdtscp();
    validator.serialize();
    stop = Cycles::rdtscp();
    GTEST_COUT << "Serialize local txs: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    printTxEntryCommits(size);

    freeTxEntry(size);
}

TEST_F(ValidatorTest, BATPeerInfo) {

	fillTxEntry(5, 10, 2); //5 txs of 10 keys and 2 peers

    validator.isUnderTest = true; //so that serialize loop will end when queue is empty

    //start cross-validation but leave it unfinished
	validator.serialize();
	EXPECT_EQ(TxEntry::TX_PENDING, txEntry[0]->getTxState());

	for (int ent = 0; ent < 5; ent++) {
		validator.peerInfo.add(txEntry[ent]);

		EXPECT_NE(txEntry[ent]->getPeerSet(), txEntry[ent]->getPeerSeenSet());
	}

	for (int ent = 1; ent < 4; ent++) {
		for (uint64_t peer = 0; peer < 2; peer++) {
			EXPECT_EQ(TxEntry::TX_PENDING, txEntry[ent]->getTxState());
			helper.updatePeerInfo(txEntry[ent]->getCTS(), peer, 0, 0xfffffff);
		}
		EXPECT_NE(TxEntry::TX_PENDING, txEntry[ent]->getTxState());
	}
	EXPECT_EQ((uint32_t)5, validator.peerInfo.size());
	validator.peerInfo.sweep();
	EXPECT_EQ((uint32_t)2, validator.peerInfo.size());

	freeTxEntry(5);
}*/


TEST_F(ValidatorTest, BATDistributedTxSetPerf) {
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty

    int size = (int)(sizeof(txEntry) / sizeof(TxEntry *));
    uint64_t start, stop;

    fillTxEntry(size, 20);

    //time all operations
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.distributedTxSet.add(txEntry[i]);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "distributedTxSet.add (" << size << ") [" <<  validator.distributedTxSet.count()
    		<< "ok]:  Total cycles " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per add: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    EXPECT_EQ(10, (int)validator.distributedTxSet.independentQueueCount());
    EXPECT_EQ(validator.distributedTxSet.count(),
    		validator.distributedTxSet.independentQueueCount() +
			validator.distributedTxSet.coldQueueCount() +
			validator.distributedTxSet.hotQueueCount());

    GTEST_COUT << "independ: " << validator.distributedTxSet.independentQueueCount()
    		<< "; cold: " << validator.distributedTxSet.coldQueueCount()
			<< "; hot: " << validator.distributedTxSet.hotQueueCount()
			<< std::endl;

    start = Cycles::rdtscp();
    validator.distributedTxSet.findReadyTx(validator.activeTxSet);
    stop = Cycles::rdtscp();
    GTEST_COUT << "findReadyTx (" << validator.distributedTxSet.count() << "): Total cycles " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per try: " << (Cycles::toSeconds(stop - start) / validator.distributedTxSet.count())  << std::endl;

	freeTxEntry(size);
}

/*
TEST_F(ValidatorTest, BATDependencyMatrix) {
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty

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
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty

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
