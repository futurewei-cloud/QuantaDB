/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "MockCluster.h"
#include "LeaseCommon.h"
#include "Validator.h"
#include "Tub.h"
#include "MultiWrite.h"
#include "Cycles.h"

#include <ostream>
#define GTEST_COUT  std::cerr << "[ INFO ] "

namespace RAMCloud {

using namespace DSSN;

class ValidatorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ClusterClock clusterClock;
    DSSN::Validator validator;
    TxEntry txEntry[1000000];
    Tub<Object> objectFromVoidPointer;
    char stringKey[256];
    uint8_t dataBlob[512];
    Buffer buffer3;

    ValidatorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , clusterClock()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ValidatorTest);

    void fillTxEntry(int noEntries, int noKeys = 1, int keySize = 3) {
        KVLayout kv(keySize);
        snprintf((char *)kv.k.key.get(), 3, "ha");
        kv.k.keyLength = 3;
        kv.v.valuePtr = (uint8_t *)dataBlob;
        kv.v.valueLength = sizeof(dataBlob);
        KVLayout *kvOut = validator.kvStore.preput(kv);
    	memset(txEntry, 0, sizeof(txEntry)); //FIXME: watch out for mem leak
        for (int i = 0; i < noEntries; i++) {
        	for (int j = 0; j < noKeys; j++) {
        		if (j % 5 == 0)
        			txEntry[i].writeSet.push_back(kvOut);
        		else
        			txEntry[i].readSet.push_back(kvOut);
        	}
        }
    }
};

TEST_F(ValidatorTest, BATKVStorePutGet) {
	fillTxEntry(1);
	for (uint32_t i = 0; i < txEntry[0].writeSet.size(); i++) {
		validator.kvStore.put(*txEntry[0].writeSet[i]);
		uint8_t *valuePtr = 0;
		uint32_t valueLength;
		validator.kvStore.getValue(txEntry[0].writeSet[i]->k, valuePtr, valueLength);
	    EXPECT_EQ(sizeof(dataBlob), valueLength);
	    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));
	}
}

TEST_F(ValidatorTest, BATKVStorePutGetMulti) {
	fillTxEntry(10);
	for (uint32_t i = 0; i < txEntry[0].writeSet.size(); i++) {
		validator.kvStore.put(*txEntry[0].writeSet[i]);
		uint8_t *valuePtr = 0;
		uint32_t valueLength;
		validator.kvStore.getValue(txEntry[0].writeSet[i]->k, valuePtr, valueLength);
	    EXPECT_EQ(sizeof(dataBlob), valueLength);
	    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));
	}
}
/*
TEST_F(ValidatorTest, BATKVStorePutGetVarious) {
	fillTxEntry(1);
	for (uint32_t i = 0; i < txEntry[0].writeSet.size(); i++) {
		validator.kvStore.put(*txEntry[0].writeSet[i]);
		uint8_t *valuePtr = 0;
		uint32_t valueLength;
		validator.kvStore.getValue(txEntry[0].writeSet[i]->k, valuePtr, valueLength);
	    EXPECT_EQ(sizeof(dataBlob), valueLength);
	    EXPECT_EQ(0, std::memcmp(dataBlob, valuePtr, valueLength));
	}
}*/

TEST_F(ValidatorTest, BATValidateLocalTxs) {

    int size = (int)(sizeof(txEntry) / sizeof(TxEntry));
    int count = 0;
    uint64_t start, stop;

    fillTxEntry(size);

    count = 0;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	if (validator.localTxQueue.push(&txEntry[i])) count++;
    }
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
    	if (validator.activeTxSet.blocks(&txEntry[i])) {
    		EXPECT_EQ(0, 1);
    	}
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "activeTxSet.blocks(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    //time validate()
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.validateLocalTx(txEntry[i]);
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

    //time all operations
    for (int i = 0; i < size; i++) {
    	validator.localTxQueue.push(&txEntry[i]);
    }
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	validator.localTxQueue.try_pop(tmp);
    	validator.activeTxSet.blocks(tmp);
    	validator.validateLocalTx(*tmp);
    	validator.conclude(*tmp);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "pop,blocks,validate,conclude: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    //time serializa()
    for (int i = 0; i < size; i++) {
    	validator.localTxQueue.push(&txEntry[i]);
    }
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    start = Cycles::rdtscp();
    validator.serialize();
    stop = Cycles::rdtscp();
    GTEST_COUT << "Serialize local txs: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
}
/*
TEST_F(ValidatorTest, BATUpdateTuple) {
    Object* singleKeyObject;
    Tub<Object> objectFromVoidPointer;
    char stringKey[3];
    char dataBlob[4];
    Buffer buffer3;
    TxEntry txEntry[1];

    snprintf(dataBlob, sizeof(dataBlob), "YO!");
    snprintf(stringKey, sizeof(stringKey), "ha");
    Key key(57, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3, 123, 723, buffer3);
    singleKeyObject = &*objectFromVoidPointer;

    EXPECT_EQ(3, (int)singleKeyObject->getValueLength());
    validator.updateTuple(*singleKeyObject, txEntry[0]);
    const string* tupleValue = validator.getTupleValue(*singleKeyObject);
    EXPECT_EQ("YO!", *tupleValue);
    EXPECT_EQ(3, (int)tupleValue->size());
	//fillTxEntry(1);
	/EXPECT_EQ(3, (int)txEntry[0].writeTuples[0]->getValueLength());
    //validator.updateTuple(*txEntry[0].writeTuples[0], txEntry[0]);
    //const string* tupleValue = validator.getTupleValue(*txEntry[0].writeTuples[0]);
    //EXPECT_EQ("YO!", *tupleValue);
    //EXPECT_EQ(3, (int)tupleValue->size());
}

TEST_F(ValidatorTest, BATValidateLocalTx) {
    Object* singleKeyObject;
    Tub<Object> objectFromVoidPointer;
    char stringKey[3];
    char dataBlob[4];
    Buffer buffer3;
    TxEntry txEntry[1];

    snprintf(dataBlob, sizeof(dataBlob), "YO!");
    snprintf(stringKey, sizeof(stringKey), "ha");
    Key key(57, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3, 123, 723, buffer3);
    singleKeyObject = &*objectFromVoidPointer;
    txEntry[0].writeTuples.push_back(singleKeyObject);


   // fillTxEntry(1);
    validator.localTxQueue.push(&txEntry[0]);

    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry[0].txState); //COMMIT
    const string* tupleValue = validator.getTupleValue(*txEntry[0].writeTuples[0]);

    EXPECT_EQ("YO!", *tupleValue);
    EXPECT_EQ(3, (int)tupleValue->size());
}

TEST_F(ValidatorTest, BATValidateLocalTxs) {
    Object* singleKeyObject;
    Tub<Object> objectFromVoidPointer;
    char stringKey[3];
    char dataBlob[4];
    Buffer buffer3;

    snprintf(dataBlob, sizeof(dataBlob), "YO!");
    snprintf(stringKey, sizeof(stringKey), "ha");
    Key key(57, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3, 123, 723, buffer3);
    singleKeyObject = &*objectFromVoidPointer;
    int size = (int)(sizeof(txEntry) / sizeof(TxEntry));
    int count = 0;
    uint64_t start, stop;

    //time push()
    for (int i = 0; i < size; i++) {
    	txEntry[i].writeTuples.push_back(singleKeyObject);
    }
    count = 0;
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	if (validator.localTxQueue.push(&txEntry[i])) count++;
    }
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
    	if (validator.activeTxSet.blocks(&txEntry[i])) {
    		EXPECT_EQ(0, 1);
    	}
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "activeTxSet.blocks(): Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    //time validate()
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	validator.validateLocalTx(txEntry[i]);
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

    //time all operations
    for (int i = 0; i < size; i++) {
    	txEntry[i].writeTuples.push_back(singleKeyObject);
    	validator.localTxQueue.push(&txEntry[i]);
    }
    start = Cycles::rdtscp();
    for (int i = 0; i < size; i++) {
    	TxEntry *tmp;
    	validator.localTxQueue.try_pop(tmp);
    	validator.activeTxSet.blocks(tmp);
    	validator.validateLocalTx(*tmp);
    	validator.conclude(*tmp);
    }
    stop = Cycles::rdtscp();
    GTEST_COUT << "pop,blocks,validate,conclude: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;

    //time serializa()
    for (int i = 0; i < size; i++) {
    	txEntry[i].writeTuples.push_back(singleKeyObject);
    	validator.localTxQueue.push(&txEntry[i]);
    }
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    start = Cycles::rdtscp();
    validator.serialize();
    stop = Cycles::rdtscp();
    GTEST_COUT << "Serialize local txs: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
}*/

}  // namespace RAMCloud
