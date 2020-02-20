/* Copyright (c) 2020  Futurewei Technologies, Inc.
 *
 */

#include "TestUtil.h"
#include "MockCluster.h"
#include "LeaseCommon.h"
#include "Validator.h"
#include "Tub.h"
#include "MultiWrite.h"

namespace RAMCloud {

using namespace DSSN;

class ValidatorTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    ClusterClock clusterClock;
    DSSN::Validator validator;

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
    EXPECT_EQ(1, (int)validator.localTxQueue.unsafe_size());
    validator.isUnderTest = true; //so that serialize loop will end when queue is empty
    validator.serialize();
    EXPECT_EQ(3, (int)txEntry.txState); //COMMIT
    const string* tupleValue = validator.getTupleValue(*singleKeyObject);
    EXPECT_EQ("YO!", *tupleValue);
    EXPECT_EQ(3, (int)tupleValue->size());
}

}  // namespace RAMCloud
