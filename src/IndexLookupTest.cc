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

/* Copyright (c) 2014-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "IndexKey.h"
#include "IndexLookup.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "RawMetrics.h"
#include "ServerMetrics.h"
#include "StringUtil.h"

namespace RAMCloud {
// This class provides tablet map and indexlet map info to ObjectFinder.
class IndexLookupRpcRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    IndexLookupRpcRefresher() : called(0) {}
    bool tryGetTableConfig(
            uint64_t tableId,
            std::map<TabletKey, TabletWithLocator>* tableMap,
            std::multimap<std::pair<uint64_t, uint8_t>,
                          IndexletWithLocator>* tableIndexMap) {

        called++;
        char buffer[100];
        uint64_t numTablets = 10;
        uint8_t numIndexlets = 26;

        tableMap->clear();
        uint64_t tabletRange = 1 + ~0UL / numTablets;
        for (uint64_t i = 0; i < numTablets; i++) {
            uint64_t startKeyHash = i * tabletRange;
            uint64_t endKeyHash = startKeyHash + tabletRange - 1;
            snprintf(buffer, sizeof(buffer), "mock:dataserver=%lu", i);
            if (i == (numTablets - 1))
                endKeyHash = ~0UL;
            Tablet rawEntry({10, startKeyHash, endKeyHash, ServerId(),
                            Tablet::NORMAL, LogPosition()});
            TabletWithLocator entry(rawEntry, buffer);

            TabletKey key {entry.tablet.tableId, entry.tablet.startKeyHash};
            tableMap->insert(std::make_pair(key, entry));
        }

        tableIndexMap->clear();
        for (uint8_t i = 0; i < numIndexlets; i++) {
            auto id = std::make_pair(10, 1); // Pair of table id and index id.
            char firstKey = static_cast<char>('a'+i);
            char firstNotOwnedKey = static_cast<char>('b'+i);
            snprintf(buffer, sizeof(buffer), "mock:indexserver=%u", i);
            IndexletWithLocator indexlet(
                reinterpret_cast<void *>(&firstKey), 1,
                reinterpret_cast<void *>(&firstNotOwnedKey), 1,
                buffer);
            tableIndexMap->insert(std::make_pair(id, indexlet));
        }
        return true;
    }
    uint32_t called;
};

class IndexLookupTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Tub<RamCloud> ramcloud;
    Context context;
    MockCluster cluster;
    IndexletManager* im;
    Tub<MockTransport> transport;
    IndexKey::IndexKeyRange azKeyRange;

    IndexLookupTest()
        : logEnabler()
        , ramcloud()
        , context()
        , cluster(&context)
        , im()
        , transport()
        , azKeyRange(1, "a", 1, "z", 1)
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
			   WireFormat::ADMIN_SERVICE,
                           WireFormat::DSSN_SERVICE};
        config.localLocator = "mock:host=master1";
        cluster.addServer(config);

        config.services = {WireFormat::ADMIN_SERVICE};
        config.localLocator = "mock:host=ping1";
        cluster.addServer(config);

        im = &cluster.contexts[0]->getMasterService()->indexletManager;
        ramcloud.construct("mock:");
        transport.construct(ramcloud->clientContext);

        ramcloud->clientContext->objectFinder->tableConfigFetcher.reset(
                new IndexLookupRpcRefresher);
        ramcloud->clientContext->transportManager->registerMock(
                transport.get());

    }

    ~IndexLookupTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(IndexLookupTest);
};

TEST_F(IndexLookupTest, construction) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    EXPECT_EQ("mock:indexserver=0",
        indexLookup.lookupRpc.rpc->session->serviceLocator);
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
}

// Rule 1:
// Handle the completion of a lookIndexKeys RPC.
TEST_F(IndexLookupTest, isReady_lookupComplete) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    const char *nextKey = "next key for rpc";
    size_t nextKeyLen = strlen(nextKey) + 1; // include null char

    Buffer *respBuffer = indexLookup.lookupRpc.rpc->response;

    respBuffer->emplaceAppend<WireFormat::ResponseCommon>()->status = STATUS_OK;
    // numHashes
    respBuffer->emplaceAppend<uint32_t>(10);
    // nextKeyLength
    respBuffer->emplaceAppend<uint16_t>(uint16_t(nextKeyLen));
    // nextKeyHash
    respBuffer->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        respBuffer->emplaceAppend<KeyHash>(i);
    }
    respBuffer->appendCopy(nextKey, (uint32_t) nextKeyLen);

    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(10U, indexLookup.lookupRpc.numHashes + indexLookup.numInserted);
    EXPECT_EQ(0U, indexLookup.nextKeyHash);
    EXPECT_EQ(0, strcmp(reinterpret_cast<char*>(indexLookup.nextKey), nextKey));
}

// Rule 2:
// Copy PKHashes from response buffer of lookup RPC into activeHashes
TEST_F(IndexLookupTest, isReady_activeHashes) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
        WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::FREE, indexLookup.lookupRpc.status);
    for (KeyHash i = 0; i < 10; i++) {
        EXPECT_EQ(i, indexLookup.activeHashes[i]);
    }
}

// Rule 3(a):
// Issue next lookup RPC if an RESULT_READY lookupIndexKeys RPC
// has no unread RPC
TEST_F(IndexLookupTest, isReady_issueNextLookup) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
            WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(1));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->response->emplaceAppend<char>('b');
    EXPECT_EQ("mock:indexserver=0",
                indexLookup.lookupRpc.rpc->session->serviceLocator);
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    EXPECT_EQ("mock:indexserver=1",
            indexLookup.lookupRpc.rpc->session->serviceLocator);
}

// Rule 3(b):
// If all lookup RPCs have completed, mark finishedLookup as true, which
// indicates no outgoing lookup RPC thereafter.
TEST_F(IndexLookupTest, isReady_allLookupCompleted) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
            WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ(IndexLookup::FREE, indexLookup.lookupRpc.status);
    EXPECT_TRUE(indexLookup.finishedLookup);
}

// Rule 5:
// Try to assign the current key hash to an existing RPC to the same server.
TEST_F(IndexLookupTest, isReady_assignPKHashesToSameServer) {
    TestLog::Enable _;
    IndexLookup indexLookup(ramcloud.get(), 10, azKeyRange);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<
        WireFormat::ResponseCommon>()->status = STATUS_OK;
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint32_t>(10);
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint16_t>(uint16_t(0));
    indexLookup.lookupRpc.rpc->response->emplaceAppend<uint64_t>(0);
    for (KeyHash i = 0; i < 10; i++) {
        indexLookup.lookupRpc.rpc->response->emplaceAppend<KeyHash>(i);
    }
    indexLookup.lookupRpc.rpc->completed();
    EXPECT_EQ(IndexLookup::SENT, indexLookup.lookupRpc.status);
    indexLookup.isReady();
    EXPECT_EQ("mock:dataserver=0",
               indexLookup.readRpcs[0].rpc->session->serviceLocator);
    EXPECT_EQ(10U, indexLookup.readRpcs[0].numHashes);
    EXPECT_EQ(9U, indexLookup.readRpcs[0].maxPos);
    EXPECT_EQ(IndexLookup::SENT, indexLookup.readRpcs[0].status);
}

// Adds bogus index entries for an object that shouldn't be in range query.
TEST_F(IndexLookupTest, getNext_filtering) {
    ramcloud.construct(&context, "mock:host=coordinator");
    uint64_t tableId = ramcloud->createTable("table");
    ramcloud->createIndex(tableId, 1, 0);

    uint8_t numKeys = 2;

    KeyInfo keyList1[2];
    keyList1[0].keyLength = 11;
    keyList1[0].key = "primaryKey1";
    keyList1[1].keyLength = 1;
    keyList1[1].key = "a";

    KeyInfo keyList2[2];
    keyList2[0].keyLength = 11;
    keyList2[0].key = "primaryKey2";
    keyList2[1].keyLength = 1;
    keyList2[1].key = "b";

    KeyInfo keyList3[2];
    keyList3[0].keyLength = 11;
    keyList3[0].key = "primaryKey3";
    keyList3[1].keyLength = 1;
    keyList3[1].key = "c";

    ramcloud->write(tableId, numKeys, keyList1, "value1");
    ramcloud->write(tableId, numKeys, keyList2, "value2");
    ramcloud->write(tableId, numKeys, keyList3, "value3");

    Key primaryKey1(tableId, keyList1[0].key, keyList1[0].keyLength);
    uint64_t pkhash1 = primaryKey1.getHash();
    // Insert extra index entries corresponding to pkhash for primaryKey1.
    // We will ensure that these don't get returned on lookup.
    im->insertEntry(tableId, 1, "b1", 2, pkhash1);
    im->insertEntry(tableId, 1, "c1", 2, pkhash1);
    im->insertEntry(tableId, 1, "B1", 2, pkhash1);
    im->insertEntry(tableId, 1, "C1", 2, pkhash1);

    // Lookup for a key range such that the range contains some real index
    // entries (corresponding to objects) and some fake index entries (inserted
    // artificially above). Test that the objects get returned.

    IndexKey::IndexKeyRange keyRange1(1, "b", 1, "d", 1);
    IndexLookup indexLookup1(ramcloud.get(), tableId, keyRange1);

    EXPECT_TRUE(indexLookup1.getNext());
    Object* obj = indexLookup1.currentObject();
    EXPECT_STREQ("primaryKey2", StringUtil::binaryToString(
            obj->getKey(), obj->getKeyLength(0)).c_str());

    EXPECT_TRUE(indexLookup1.getNext());
    obj = indexLookup1.currentObject();
    EXPECT_STREQ("primaryKey3", StringUtil::binaryToString(
            obj->getKey(), obj->getKeyLength(0)).c_str());

    EXPECT_FALSE(indexLookup1.getNext());

    // Lookup for a key range such that it contains only fake index entries.
    // Test that nothing gets returned.
    IndexKey::IndexKeyRange keyRange2(1, "B", 1, "D", 1);
    IndexLookup indexLookup2(ramcloud.get(), tableId, keyRange2);

    EXPECT_FALSE(indexLookup2.getNext());
}
} // namespace ramcloud
