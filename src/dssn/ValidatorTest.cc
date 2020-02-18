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
    Key key(57, stringKey, sizeof(stringKey));

    objectFromVoidPointer.construct(key, dataBlob, 4, 75, 723, buffer3);

    singleKeyObject = &*objectFromVoidPointer;

    validator.updateTuple(*singleKeyObject, txEntry);
    EXPECT_EQ("YO!", *validator.getTupleValue(*singleKeyObject));
}

}  // namespace RAMCloud
