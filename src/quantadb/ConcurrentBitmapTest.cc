/* Copyright 2021 Futurewei Technologies, Inc.
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
#include "TestLog.h"
#include "ConcurrentBitmap.h"

using namespace RAMCloud;

class ConcurrentBitmapTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    QDB::ConcurrentBitmap *cBitmap;
    explicit ConcurrentBitmapTest()
        : logEnabler()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
	cBitmap = new QDB::ConcurrentBitmap();
    }

    DISALLOW_COPY_AND_ASSIGN(ConcurrentBitmapTest);
};

TEST_F(ConcurrentBitmapTest, basic) {
    uint64_t hash = 8;
    EXPECT_TRUE(cBitmap->set(hash));
    EXPECT_TRUE(cBitmap->isSet(hash));
    EXPECT_TRUE(cBitmap->unset(hash));
    EXPECT_FALSE(cBitmap->isSet(hash));
    hash = 128;
    EXPECT_TRUE(cBitmap->set(hash));
    EXPECT_TRUE(cBitmap->isSet(hash));
    EXPECT_TRUE(cBitmap->unset(hash));
    EXPECT_FALSE(cBitmap->isSet(hash));
    hash = (uint64_t)-1;
    EXPECT_TRUE(cBitmap->set(hash));
    EXPECT_TRUE(cBitmap->isSet(hash));
    EXPECT_TRUE(cBitmap->unset(hash));
    EXPECT_FALSE(cBitmap->isSet(hash));
}

TEST_F(ConcurrentBitmapTest, adjacent) {
    uint64_t hash = 0;
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->set(hash));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->isSet(hash));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->unset(hash));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_FALSE(cBitmap->isSet(hash));
    }
    
}

TEST_F(ConcurrentBitmapTest, large) {
    uint64_t hash = 0;
    uint64_t base = 1024*1024;
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->set(hash+base));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->isSet(hash+base));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_TRUE(cBitmap->unset(hash+base));
    }
    for (hash = 0; hash <1024; hash++) {
        EXPECT_FALSE(cBitmap->isSet(hash+base));
    }
}
