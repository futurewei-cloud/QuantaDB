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

    ValidatorTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , clusterClock()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ValidatorTest);
};

TEST_F(ValidatorTest, BATUpdateTuple) {
    Object* singleKeyObject;
    Tub<Object> objectFromVoidPointer;
    char stringKey[3];
    char dataBlob[4];
    Buffer buffer3;
    TxEntry txEntry;

    snprintf(dataBlob, sizeof(dataBlob), "YO!");
    snprintf(stringKey, sizeof(stringKey), "ha");
    Key key(57 /*tableId*/, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3 /*value length*/, 123 /*version*/, 723 /*timestamp*/, buffer3);
    singleKeyObject = &*objectFromVoidPointer;

    EXPECT_EQ(3, (int)singleKeyObject->getValueLength());
    validator.updateTuple(*singleKeyObject, txEntry);
    const string* tupleValue = validator.getTupleValue(*singleKeyObject);
    EXPECT_EQ("YO!", *tupleValue);
    EXPECT_EQ(3, (int)tupleValue->size());
}

TEST_F(ValidatorTest, BATValidateLocalTx) {
    Object* singleKeyObject;
    Tub<Object> objectFromVoidPointer;
    char stringKey[3];
    char dataBlob[4];
    Buffer buffer3;
    TxEntry txEntry;

    snprintf(dataBlob, sizeof(dataBlob), "YO!");
    snprintf(stringKey, sizeof(stringKey), "ha");
    Key key(57 /*tableId*/, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3, 123, 723, buffer3);
    singleKeyObject = &*objectFromVoidPointer;
    txEntry.writeSet.push_back(singleKeyObject);
    validator.localTxQueue.push(&txEntry);
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry.txState); //COMMIT
    const string* tupleValue = validator.getTupleValue(*singleKeyObject);
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
    Key key(57 /*tableId*/, stringKey, sizeof(stringKey));
    objectFromVoidPointer.construct(key, dataBlob, 3, 123, 723, buffer3);
    singleKeyObject = &*objectFromVoidPointer;
    int size = (int)(sizeof(txEntry) / sizeof(TxEntry));
    int count = 0;
    uint64_t start, stop;

    //time pop()
    count = 0;
    for (int i = 0; i < size; i++) {
    	txEntry[i].writeSet.push_back(singleKeyObject);
    	if (validator.localTxQueue.push(&txEntry[i])) count++;
    }
    EXPECT_EQ(size, count);
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
    	txEntry[i].writeSet.push_back(singleKeyObject);
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
    	txEntry[i].writeSet.push_back(singleKeyObject);
    	validator.localTxQueue.push(&txEntry[i]);
    }
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    start = Cycles::rdtscp();
    validator.serialize();
    stop = Cycles::rdtscp();
    GTEST_COUT << "Serialize local txs: Total cycles (" << size << " txs): " << (stop - start) << std::endl;
    GTEST_COUT << "Sec per local tx: " << (Cycles::toSeconds(stop - start) / size)  << std::endl;
}

}  // namespace RAMCloud
