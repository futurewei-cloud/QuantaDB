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

/* Copyright (c) 2015-2016 Stanford University
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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "ClientTransactionTask.h"
#include "MockCluster.h"
#include "Transaction.h"
#include "MultiOp.h"

namespace RAMCloud {

class TransactionTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    uint64_t tableId1;
    uint64_t tableId2;
    uint64_t tableId3;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<Transaction> transaction;
    ClientTransactionTask* task;

    TransactionTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , tableId1(-1)
        , tableId2(-2)
        , tableId3(-3)
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , transaction()
        , task()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.services = {WireFormat::MASTER_SERVICE,
			   WireFormat::ADMIN_SERVICE,
                           WireFormat::DSSN_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
			   WireFormat::ADMIN_SERVICE,
                           WireFormat::DSSN_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
			   WireFormat::ADMIN_SERVICE,
                           WireFormat::DSSN_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        transaction.construct(ramcloud.get());
        task = transaction->taskPtr.get();

        // Make some tables.
        tableId1 = ramcloud->createTable("table1");
        tableId2 = ramcloud->createTable("table2");
        tableId3 = ramcloud->createTable("table3");
    }

    DISALLOW_COPY_AND_ASSIGN(TransactionTest);
};

TEST_F(TransactionTest, commit_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, client_early_abort) {
    bool exist = false;
    Buffer value;
    transaction->read(tableId1, "0", 1, &value, &exist);
    transaction->write(tableId1, "1", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_FALSE(transaction->commit());
}


TEST_F(TransactionTest, commit_abort) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_FALSE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);

    // Check that commit does not wait for decision rpcs to return.
    transaction->taskPtr.get()->state = ClientTransactionTask::DECISION;
    EXPECT_FALSE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DECISION,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
#endif
}

TEST_F(TransactionTest, commit_internalError) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    transaction->commit();
    transaction->taskPtr.get()->decision = WireFormat::TxDecision::UNDECIDED;
    EXPECT_THROW(transaction->commit(), InternalError);
#endif
}

TEST_F(TransactionTest, sync_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    transaction->sync();
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, sync_beforeCommit) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    transaction->sync();
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_TRUE(transaction->commit());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commitAndSync_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_TRUE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
}

TEST_F(TransactionTest, commitAndSync_abort) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->write(tableId1, "0", 1, "hello", 5);

    EXPECT_FALSE(transaction->commitStarted);
    EXPECT_EQ(ClientTransactionTask::INIT,
              transaction->taskPtr.get()->state);
    EXPECT_FALSE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
    EXPECT_FALSE(transaction->commitAndSync());
    EXPECT_EQ(ClientTransactionTask::DONE,
              transaction->taskPtr.get()->state);
    EXPECT_TRUE(transaction->commitStarted);
#endif
}

TEST_F(TransactionTest, read_basic) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    EXPECT_TRUE(task->readOnly);
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    transaction->read(tableId1, "0", 1, &value);
    EXPECT_TRUE(task->readOnly);
    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    const char* str;
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("abcdef", string(str, dataLength));
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    //TODO: Check the eta & pi value
}

TEST_F(TransactionTest, read_noObject) {
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    entry->objectBuf.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_TRUE(entry->rejectRules.exists);

    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, read_afterWrite) {
    uint32_t dataLength = 0;
    const char* str;

    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    // Make sure the read, reads the last write.
    Buffer value;
    transaction->read(1, "test", 4, &value);
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    // Make sure the operations is still cached as a write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));
}

TEST_F(TransactionTest, read_afterRemove) {
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->remove(1, "test", 4);

    // Make read throws and exception following a remove.
    Buffer value;
    EXPECT_THROW(transaction->read(1, "test", 4, &value),
                 ObjectDoesntExistException);
}

TEST_F(TransactionTest, read_afterCommit) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    transaction->commitStarted = true;

    Buffer value;
    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, read_modify_Write) {
    uint32_t dataLength = 0;
    const char* str;

    Transaction t0(ramcloud.get());
    Key key(1, "test_rmw", 8);

    t0.write(1, "test_rmw", 8, "hello", 5);
    EXPECT_TRUE(t0.commit());

    // Make sure the read, reads the last write.
    Buffer value;
    transaction->read(1, "test_rmw", 8, &value);
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    transaction->write(1, "test_rmw", 8, "hello2", 6);
    // Make sure the operations is cached as a read_modify_write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ_MODIFY_WRITE, entry->type);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("hello2", string(str, dataLength));
    EXPECT_TRUE(transaction->commit());
}

TEST_F(TransactionTest, read_modify_Write2) {
    uint32_t dataLength = 0;
    const char* str;

    Transaction t0(ramcloud.get());
    Key key(1, "test_rmw2", 9);

    t0.write(1, "test_rmw2", 9, "hello", 5);
    EXPECT_TRUE(t0.commit());

    // Make sure the read, reads the last write.
    Buffer value;
    transaction->read(1, "test_rmw2", 9, &value);
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    transaction->write(1, "test_rmw2", 9, "hello2", 6);
    transaction->write(1, "test_rmw2", 9, "hello3", 6);
    // Make sure the operations is cached as a read_modify_write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ_MODIFY_WRITE, entry->type);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("hello3", string(str, dataLength));
    EXPECT_TRUE(transaction->commit());
}

TEST_F(TransactionTest, remove) {
    EXPECT_TRUE(task->readOnly);
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->remove(1, "test", 4);

    EXPECT_FALSE(task->readOnly);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);

    transaction->write(1, "test", 4, "goodbye", 7);
    entry->rejectRules.givenVersion = 42;

    transaction->remove(1, "test", 4);

    EXPECT_EQ(ClientTransactionTask::CacheEntry::REMOVE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);

    EXPECT_EQ(entry, task->findCacheEntry(key));
}

TEST_F(TransactionTest, remove_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->remove(1, "test", 4),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, remove_commit_read) {
    Buffer value1, value2, value3, value4, value5, value;
    ramcloud->write(tableId1, "key0", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key1", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key2", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key3", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key4", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key5", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key6", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key7", 4, "abcdef", 6);
    ramcloud->write(tableId1, "key8", 4, "abcdef", 6);

    transaction->read(tableId1, "key1", 4, &value1);
    transaction->read(tableId1, "key2", 4, &value2);
    transaction->read(tableId1, "key3", 4, &value3);
    transaction->read(tableId1, "key4", 4, &value4);
    transaction->read(tableId1, "key5", 4, &value5);
    transaction->write(tableId1, "key9", 4, "abcdef", 6);

    transaction->remove(tableId1, "key8", 4);

    transaction->commit();
    EXPECT_THROW(ramcloud->read(tableId1, "key8", 4, &value),
		 ObjectDoesntExistException);
}

TEST_F(TransactionTest, write) {
    uint32_t dataLength = 0;
    const char* str;

    EXPECT_TRUE(task->readOnly);
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    EXPECT_FALSE(task->readOnly);
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));

    entry->type = ClientTransactionTask::CacheEntry::INVALID;
    entry->rejectRules.givenVersion = 42;

    transaction->write(1, "test", 4, "goodbye", 7);

    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(42U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("goodbye", string(str, dataLength));

    EXPECT_EQ(entry, task->findCacheEntry(key));
}

TEST_F(TransactionTest, write_afterCommit) {
    transaction->commitStarted = true;
    EXPECT_THROW(transaction->write(1, "test", 4, "hello", 5),
                 TxOpAfterCommit);
}

TEST_F(TransactionTest, write_skew) {

    // set the data for running the transactions
    Buffer value,value1,value2,value3;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "30", 2);
    ramcloud->write(tableId1, "Y", 1, "10", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());

    t1.read(tableId1, "X", 1, &value);
    EXPECT_EQ("30", string(reinterpret_cast<const char*>(value.getRange(0, value.size())),value.size()));

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());

    t2.read(tableId1, "Y", 1, &value1);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    t1.write(tableId1, "Y", 1, "60", 2);

    EXPECT_TRUE(t1.commit());

    // Transaction t1 END

    t2.write(tableId1, "X", 1, "50", 2);

    EXPECT_FALSE(t2.commit());

    // Transaction t2 END

    ramcloud->read(tableId1, "X", 1, &value2);
    EXPECT_EQ("30", string(reinterpret_cast<const char*>(value2.getRange(0, value2.size())),value2.size()));

    ramcloud->read(tableId1, "Y", 1, &value3);
    EXPECT_EQ("60", string(reinterpret_cast<const char*>(value3.getRange(0, value3.size())),value3.size()));
}

TEST_F(TransactionTest, read_skew) {
    printf("READ_SKEW BEGIN\n");

    // set the data for running the transactions
    Buffer value,value1,value2,value3;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "50", 2);
    ramcloud->write(tableId1, "Y", 1, "50", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());

    t1.read(tableId1, "X", 1, &value);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value.getRange(0, value.size())),value.size()));

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());

    t2.write(tableId1, "X", 1, "25", 2);
    t2.write(tableId1, "Y", 1, "75", 2);

    EXPECT_TRUE(t2.commit());
    // Transaction t2 END

    t1.read(tableId1, "Y", 1, &value1);
    EXPECT_EQ("75", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    EXPECT_FALSE(t1.commit());
    // Transaction t1 END

    // result from the KVstore
    ramcloud->read(tableId1, "X", 1, &value2);
    EXPECT_EQ("25", string(reinterpret_cast<const char*>(value2.getRange(0, value2.size())),value2.size()));

    ramcloud->read(tableId1, "Y", 1, &value3);
    EXPECT_EQ("75", string(reinterpret_cast<const char*>(value3.getRange(0, value3.size())),value3.size()));
}

TEST_F(TransactionTest, lost_update) {
    // set the data for running the transactions
    Buffer value,value1;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "10", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());

    t1.read(tableId1, "X", 1, &value);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value.getRange(0, value.size())),value.size()));

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());

    t2.read(tableId1, "X", 1, &value1);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    t2.write(tableId1, "X", 1, "20", 2);

    EXPECT_TRUE(t2.commit());
    // Transaction t2 END

    t1.write(tableId1, "X", 1, "30", 2);

    EXPECT_FALSE(t1.commit());
    // Transaction t1 END
}

TEST_F(TransactionTest, fuzzy_read) {
    // set the data for running the transactions
    Buffer value,value1,value2,value3;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "50", 2);
    ramcloud->write(tableId1, "Y", 1, "50", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());

    t1.read(tableId1, "X", 1, &value);

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());

    t2.read(tableId1, "X", 1, &value1);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    t2.write(tableId1, "X", 1, "10", 2);

    t2.read(tableId1, "Y", 1, &value2);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value2.getRange(0, value2.size())),value2.size()));

    t2.write(tableId1, "Y", 1, "90", 2);

    EXPECT_TRUE(t2.commit());
    // Transaction t2 END

    t1.read(tableId1, "Y", 1, &value3);
    EXPECT_EQ("90", string(reinterpret_cast<const char*>(value3.getRange(0, value3.size())),value3.size()));

    EXPECT_FALSE(t1.commit());
    // Transaction t1 END
}

TEST_F(TransactionTest, dirty_write) {

    // set the data for running the transactions
    Buffer value1,value2;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "50", 2);
    ramcloud->write(tableId1, "Y", 1, "50", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());

    t1.write(tableId1, "X", 1, "10", 2);

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());
    t2.write(tableId1, "X", 1, "20", 2);
    t2.write(tableId1, "Y", 1, "20", 2);
    EXPECT_TRUE(t2.commit());
    // Transaction t2 END

    t1.write(tableId1, "Y", 1, "10", 2);
    EXPECT_TRUE(t1.commit());
    // Transaction t1 END

    ramcloud->read(tableId1, "X", 1, &value1);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    ramcloud->read(tableId1, "Y", 1, &value2);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value2.getRange(0, value2.size())),value2.size()));
}

TEST_F(TransactionTest, dirty_read) {
    // set the data for running the transactions
    Buffer value,value1,value2,value3,value4,value5;

    // write initial items for the table
    ramcloud->write(tableId1, "X", 1, "50", 2);
    ramcloud->write(tableId1, "Y", 1, "50", 2);

    // Transaction t1 BEGIN
    Transaction t1(ramcloud.get());
    t1.read(tableId1, "X", 1, &value);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value.getRange(0, value.size())),value.size()));

    t1.write(tableId1, "X", 1, "10", 2);

    // Transaction t2 BEGIN
    Transaction t2(ramcloud.get());
    t2.read(tableId1, "X", 1, &value1);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value1.getRange(0, value1.size())),value1.size()));

    t2.read(tableId1, "Y", 1, &value2);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value2.getRange(0, value2.size())),value2.size()));

    EXPECT_TRUE(t2.commit());
    // Transaction t2 END

    t1.read(tableId1, "Y", 1, &value3);
    EXPECT_EQ("50", string(reinterpret_cast<const char*>(value3.getRange(0, value3.size())),value3.size()));

    t1.write(tableId1, "Y", 1, "90", 2);

    EXPECT_TRUE(t1.commit());

    // read the final result
    ramcloud->read(tableId1, "X", 1, &value4);
    EXPECT_EQ("10", string(reinterpret_cast<const char*>(value4.getRange(0, value4.size())),value4.size()));

    ramcloud->read(tableId1, "Y", 1, &value5);
    EXPECT_EQ("90", string(reinterpret_cast<const char*>(value5.getRange(0, value5.size())),value5.size()));
}

TEST_F(TransactionTest, ReadOp_constructor_noCache) {
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    {   // Single Op
        Buffer value;
        Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
        EXPECT_TRUE(readOp.singleRequest->readRpc);
        EXPECT_FALSE(readOp.batchedRequest);
    }

    {   // Batched Op
        Buffer value;
        Transaction::ReadOp
                readOp(transaction.get(), tableId1, "0", 1, &value, true);
        EXPECT_FALSE(readOp.singleRequest);
        EXPECT_TRUE(readOp.batchedRequest->readBatchPtr);
        EXPECT_EQ(1U,
                  readOp.batchedRequest->readBatchPtr->requests.size());
    }
}

TEST_F(TransactionTest, ReadOp_constructor_cached) {
    Key key(1, "test", 4);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    transaction->write(1, "test", 4, "hello", 5);

    {   // Single Op
        Buffer value;
        Transaction::ReadOp readOp(transaction.get(), 1, "test", 4, &value);
        EXPECT_FALSE(readOp.singleRequest->readRpc);
        EXPECT_FALSE(readOp.batchedRequest);
    }

    {   // Batched Op
        Buffer value;
        Transaction::ReadOp
                readOp(transaction.get(), 1, "test", 4, &value, true);
        EXPECT_FALSE(readOp.singleRequest);
        EXPECT_FALSE(readOp.batchedRequest->readBatchPtr);
    }
}

TEST_F(TransactionTest, ReadOp_isReady_single) {
    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);
    EXPECT_TRUE(readOp.singleRequest->readRpc->isReady());
    EXPECT_TRUE(readOp.isReady());

    readOp.singleRequest->readRpc->state = RpcWrapper::IN_PROGRESS;

    EXPECT_TRUE(readOp.singleRequest->readRpc);
    EXPECT_FALSE(readOp.singleRequest->readRpc->isReady());
    EXPECT_FALSE(readOp.isReady());

    readOp.singleRequest->readRpc.destroy();

    EXPECT_FALSE(readOp.singleRequest->readRpc);
    EXPECT_TRUE(readOp.isReady());
}

TEST_F(TransactionTest, ReadOp_isReady_batched) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 1
    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);

    // Filler requests to delay progress.
    Transaction::ReadOp
            temp1(transaction.get(), tableId2, "0", 1, &value, true);
    Transaction::ReadOp
            temp2(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp3(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp4(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp5(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp6(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp7(transaction.get(), tableId3, "0", 1, &value, true);
    Transaction::ReadOp
            temp8(transaction.get(), tableId3, "0", 1, &value, true);

    EXPECT_TRUE(transaction->nextReadBatchPtr);
    EXPECT_TRUE(readOp.batchedRequest->readBatchPtr
                == transaction->nextReadBatchPtr);
    EXPECT_FALSE(readOp.batchedRequest->readBatchPtr->rpc);
    EXPECT_EQ(9U,
              readOp.batchedRequest->readBatchPtr->requests.size());

    EXPECT_FALSE(readOp.isReady());     // rpc has not been issued.

    EXPECT_TRUE(readOp.batchedRequest->readBatchPtr->rpc);
    EXPECT_FALSE(transaction->nextReadBatchPtr);

    EXPECT_FALSE(readOp.isReady());     // rpc should be in progress

    EXPECT_TRUE(readOp.isReady());      // rpc is complete

    readOp.batchedRequest->readBatchPtr.reset();
    EXPECT_FALSE(readOp.batchedRequest->readBatchPtr);

    EXPECT_TRUE(readOp.isReady());      // Mock no rpc sent implies cached.
#endif
}

TEST_F(TransactionTest, ReadOp_wait_async) {
    uint32_t dataLength = 0;
    const char* str;

    // Makes sure that the point of the read is when wait is called.
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);

    transaction->write(tableId1, "0", 1, "hello", 5);

    readOp.wait();
    EXPECT_EQ("hello", string(reinterpret_cast<const char*>(
                                value.getRange(0, value.size())),
                                value.size()));

    // Make sure the operations is still cached as a write.
    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::WRITE, entry->type);
    EXPECT_EQ(0U, entry->rejectRules.givenVersion);
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("hello", string(str, dataLength));
}

TEST_F(TransactionTest, ReadOp_wait_batch_basic) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    readOp.wait();

    EXPECT_EQ("abcdef", string(reinterpret_cast<const char*>(
                        value.getRange(0, value.size())),
                        value.size()));

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    const char* str;
    str = reinterpret_cast<const char*>(
            entry->objectBuf.getValue(&dataLength));
    EXPECT_EQ("abcdef", string(str, dataLength));
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_EQ(3U, entry->rejectRules.givenVersion);
#endif
}

TEST_F(TransactionTest, ReadOp_wait_batch_noObject) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 1
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    EXPECT_THROW(readOp.wait(),
                 ObjectDoesntExistException);

    ClientTransactionTask::CacheEntry* entry = task->findCacheEntry(key);
    EXPECT_TRUE(entry != NULL);
    uint32_t dataLength = 0;
    entry->objectBuf.getValue(&dataLength);
    EXPECT_EQ(0U, dataLength);
    EXPECT_EQ(ClientTransactionTask::CacheEntry::READ, entry->type);
    EXPECT_TRUE(entry->rejectRules.exists);

    EXPECT_THROW(transaction->read(tableId1, "0", 1, &value),
                 ObjectDoesntExistException);
#endif
}

TEST_F(TransactionTest, ReadOp_wait_batch_unexpectedStatus) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    Buffer value;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    EXPECT_TRUE(readOp.isReady());
    readOp.batchedRequest->request.status = STATUS_INTERNAL_ERROR;
    EXPECT_THROW(readOp.wait(), InternalError);
#endif
}

TEST_F(TransactionTest, ReadOp_wait_afterCommit) {
    // Makes sure that the point of the read is when wait is called.
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    Transaction::ReadOp readOp(transaction.get(), tableId1, "0", 1, &value);
    EXPECT_TRUE(readOp.singleRequest->readRpc);

    transaction->commit();

    EXPECT_THROW(readOp.wait(), TxOpAfterCommit);
#endif
}

TEST_F(TransactionTest, ReadOp_wait_objectExists_true) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    // Makes sure that the point of the read is when wait is called.
    ramcloud->write(tableId1, "0", 1, "abcdef", 6);

    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    bool objectExists = false;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    readOp.wait(&objectExists);
    EXPECT_TRUE(objectExists);
#endif
}

TEST_F(TransactionTest, ReadOp_wait_objectExists_false) {
    std::cout << "[Test Skipped]: "
	      << ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()
	      << ":"
	      << ::testing::UnitTest::GetInstance()->current_test_info()->name()
	      << std::endl;
#if 0
    Key key(tableId1, "0", 1);
    EXPECT_TRUE(task->findCacheEntry(key) == NULL);

    Buffer value;
    bool objectExists = true;
    Transaction::ReadOp
            readOp(transaction.get(), tableId1, "0", 1, &value, true);
    readOp.wait(&objectExists);
    EXPECT_FALSE(objectExists);
#endif
}

TEST_F(TransactionTest, commit_loop) {
    string x("X");
    uint64_t x_val = 0;
    string x_val_str = to_string(x_val);
    string xr_val_str;
    EXPECT_NO_THROW(ramcloud->write(tableId1, x.c_str(), x.size(), x_val_str.c_str(), x_val_str.size()));

    for (uint32_t i = 0; i < 20000; i++) {
      Buffer value;
      Transaction t(ramcloud.get());
      EXPECT_NO_THROW(t.read(tableId1, x.c_str(), x.size(), &value));
      xr_val_str = string(reinterpret_cast<const char*>(
			value.getRange(0, value.size())),
                        value.size());
      EXPECT_EQ(x_val_str, xr_val_str);
      x_val++;
      x_val_str = to_string(x_val);
      t.write(tableId1, x.c_str(), x.size(), x_val_str.c_str(), x_val_str.size());
      EXPECT_TRUE(t.commit());
    }
}

}  // namespace RAMCloud
