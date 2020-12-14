# Copyright 2020 Futurewei Technologies, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

add_library(gtest STATIC "")
target_sources(gtest
  PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-filepath.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-death-test.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-all.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-typed-test.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-test-part.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-printers.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest-port.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest_main.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../gtest/src/gtest.cc)

if(QDBTX)
  file(GLOB unittest
    quantadb/ClusterTimeServiceTest.cc
    quantadb/DataLogTest.cc
    quantadb/DLogTest.cc
    quantadb/DSSNServiceTest.cc
    quantadb/HashmapKVStoreTest.cc
    quantadb/HashmapTest.cc
    quantadb/MemStreamIoTest.cc
    quantadb/MultiOpTest.cc
    quantadb/MultiReadTest.cc
    quantadb/MultiRemoveTest.cc
    quantadb/MultiWriteTest.cc
    quantadb/RamCloudDSSNTest.cc
    quantadb/SequencerTest.cc
    quantadb/SkipListTest.cc
    quantadb/TpcCDSSNTest.cc
    quantadb/TransactionDSSNTest.cc
    quantadb/TxLogTest.cc
    quantadb/ValidatorTest.cc
    MockCluster.cc
    MockDriver.cc
    MockExternalStorage.cc
    MockTransport.cc
    TestLog.cc
    TestLog.h
    TestRunner.cc
    TestUtil.cc
    TestUtil.h
    )
else()
  file(GLOB unittest
    AbstractLogTest.cc
    AbstractServerListTest.cc
    AdminServiceTest.cc
    ArpCacheTest.cc
    AtomicTest.cc
    BackupFailureMonitorTest.cc
    BackupMasterRecoveryTest.cc
    BackupSelectorTest.cc
    BackupServiceTest.cc
    BackupStorageTest.cc
    BasicTransportTest.cc
    BitOpsTest.cc
    BoostIntrusiveTest.cc
    BufferTest.cc
    CacheTraceTest.cc
    CleanableSegmentManagerTest.cc
    ClientExceptionTest.cc
    ClientLeaseAgentTest.cc
    ClientLeaseAuthorityTest.cc
    ClientLeaseValidatorTest.cc
    ClientTransactionManagerTest.cc
    ClientTransactionTaskTest.cc
    ClusterClockTest.cc
    ClusterMetricsTest.cc
    ClusterTimeTest.cc
    CommonTest.cc
    ContextTest.cc
    CoordinatorClusterClockTest.cc
    CoordinatorRpcWrapperTest.cc
    CoordinatorServerListTest.cc
    CoordinatorServiceTest.cc
    CoordinatorSessionTest.cc
    CoordinatorUpdateManagerTest.cc
    CRamCloudTest.cc
    Crc32CTest.cc
    CyclesTest.cc
    DataBlockTest.cc
    DispatchExecTest.cc
    DispatchTest.cc
    ExternalStorageTest.cc
    FailSessionTest.cc
    FailureDetectorTest.cc
    FileLoggerTest.cc
    HashObjectManagerTest.cc
    HashTableTest.cc
    HistogramTest.cc
    HomaTransportTest.cc
    IndexKeyTest.cc
    IndexletManagerTest.cc
    IndexLookupTest.cc
    IndexRpcWrapperTest.cc
    InitializeTest.cc
    InMemoryStorageTest.cc
    IpAddressTest.cc
    KeyTest.cc
    LinearizableObjectRpcWrapperTest.cc
    LockTableTest.cc
    LogCabinStorageTest.cc
    LogCleanerTest.cc
    LogDigestTest.cc
    LogEntryRelocatorTest.cc
    LoggerTest.cc
    LogIteratorTest.cc
    LogProtectorTest.cc
    LogSegmentTest.cc
    LogTest.cc
    MacAddressTest.cc
    MakefragTest
    MasterRecoveryManagerTest.cc
    MasterServiceTest.cc
    MasterTableMetadataTest.cc
    MemoryMonitorTest.cc
    MinCopysetsBackupSelectorTest.cc
    MockCluster.cc
    MockClusterTest.cc
    MockDriver.cc
    MockExternalStorage.cc
    MockExternalStorageTest.cc
    MockTransport.cc
    MultiFileStorageTest.cc
    MultiIncrementTest.cc
    MultiOpTest.cc
    MultiReadTest.cc
    MultiRemoveTest.cc
    MultiWriteTest.cc
    NetUtilTest.cc
    ObjectBufferTest.cc
    ObjectFinderTest.cc
    ObjectPoolTest.cc
    ObjectRpcWrapperTest.cc
    ObjectTest.cc
    OptionParserTest.cc
    ParticipantListTest.cc
    PerfCounterTest.cc
    PerfStatsTest.cc
    PlusOneBackupSelectorTest.cc
    PortAlarmTest.cc
    PreparedOpTest.cc
    PriorityTaskQueueTest.cc
    ProtoBufTest.cc
    ProtoBufTest.proto
    QueueEstimatorTest.cc
    RamCloudTest.cc
    RawMetricsTest.cc
    RecoverySegmentBuilderTest.cc
    RecoveryTest.cc
    ReplicaManagerTest.cc
    ReplicatedSegmentTest.cc
    RpcLevelTest.cc
    RpcResultTest.cc
    RpcTrackerTest.cc
    RpcWrapperTest.cc
    RuntimeOptionsTest.cc
    SegletAllocatorTest.cc
    SegletTest.cc
    SegmentIteratorTest.cc
    SegmentManagerTest.cc
    SegmentTest.cc
    ServerIdRpcWrapperTest.cc
    ServerIdTest.cc
    ServerListTest.cc
    ServerMetricsTest.cc
    ServerRpcPoolTest.cc
    ServerTest.cc
    ServerTrackerTest.cc
    ServiceLocatorTest.cc
    ServiceMaskTest.cc
    ServiceTest.cc
    SessionAlarmTest.cc
    SideLogTest.cc
    SpinLockTest.cc
    StatusTest.cc
    StringUtilTest.cc
    TableEnumeratorTest.cc
    TableManagerTest.cc
    TableStatsTest.cc
    TabletManagerTest.cc
    TabletTest.cc
    TaskQueueTest.cc
    TcpTransportTest.cc
    TestLog.cc
    TestLog.h
    TestRunner.cc
    TestUtil.cc
    TestUtil.h
    TestUtilTest.cc
    ThreadIdTest.cc
    TimeTraceTest.cc
    TimeTraceUtilTest.cc
    TpcCTest.cc
    TransactionManagerTest.cc
    TransactionTest.cc
    TransportManagerTest.cc
    TransportTest.cc
    TubTest.cc
    TxDecisionRecordTest.cc
    TxRecoveryManagerTest.cc
    UdpDriverTest.cc
    UnackedRpcResultsTest.cc
    UpdateReplicationEpochTaskTest.cc
    UtilTest.cc
    VarLenArrayTest.cc
    WallTimeTest.cc
    WindowTest.cc
    WireFormatTest.cc
    WorkerManagerTest.cc
    WorkerSessionTest.cc
    WorkerTimerTest.cc
    ZooStorageTest.cc
    )
endif(QDBTX)

if(INFINIBAND)
  file(GLOB ibtransporttest
    InfAddressTest.cc
    InfRcTransportTest.cc
    InfUdDriverTest.cc
    )
endif(INFINIBAND)

link_directories(${CMAKE_CURRENT_BINARY_DIR}/../gtest/ ${CMAKE_CURRENT_BINARY_DIR}/)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/../hot/build/tbb_cmake_build/tbb_cmake_build_subdir_release)

add_executable(test ${unittest} ${ibtransporttest})

list(APPEND LIBS gtest "${CMAKE_SHARED_LINKER_FLAGS}")

target_link_libraries(test PUBLIC ${ramcloud}
                     ${LIBS})
