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

add_library(ramcloud SHARED
  AbstractLog.cc
  AbstractServerList.cc
  AdminClient.cc
  AdminService.cc
  ArpCache.cc
  BasicTransport.cc
  CacheTrace.cc
  ClientException.cc
  ClientLeaseAgent.cc
  ClientTransactionManager.cc
  ClientTransactionTask.cc
  Context.cc
  CoordinatorClient.cc
  CoordinatorRpcWrapper.cc
  CoordinatorSession.cc
  CRamCloud.cc
  Crc32C.cc
  BackupClient.cc
  BackupFailureMonitor.cc
  BackupSelector.cc
  Buffer.cc
  CleanableSegmentManager.cc
  ClientException.cc
  ClusterMetrics.cc
  CodeLocation.cc
  Common.cc
  Cycles.cc
  DataBlock.cc
  Dispatch.cc
  DispatchExec.cc
  Driver.cc
  ZooStorage.cc
  HashEnumeration.cc
  HashEnumerationIterator.cc
  ExternalStorage.cc
  FailureDetector.cc
  FailSession.cc
  FileLogger.cc
  HashObjectManager.cc
  HashTable.cc
  HomaTransport.cc
  IndexKey.cc
  IndexletManager.cc
  IndexLookup.cc
  IndexRpcWrapper.cc
  IpAddress.cc
  Key.cc
  LargeBlockOfMemory.cc
  LinearizableObjectRpcWrapper.cc
  LockTable.cc
  Log.cc
  LogCabinLogger.cc
  LogCabinStorage.cc
  LogCleaner.cc
  LogDigest.cc
  LogEntryRelocator.cc
  LogEntryTypes.cc
  LogMetricsStringer.cc
  LogProtector.cc
  Logger.cc
  LogIterator.cc
  MacAddress.cc
  MacIpAddress.cc
  MasterClient.cc
  MasterService.cc
  MasterTableMetadata.cc
  Memory.cc
  MemoryMonitor.cc
  MinCopysetsBackupSelector.cc
  MultiOp.cc
  MultiIncrement.cc
  MultiRead.cc
  MultiRemove.cc
  MultiWrite.cc
  MurmurHash3.cc
  NetUtil.cc
  Notifier.cc
  Object.cc
  ObjectBuffer.cc
  ObjectFinder.cc
  ObjectRpcWrapper.cc
  OptionParser.cc
  ParticipantList.cc
  PcapFile.cc
  PerfCounter.cc
  PerfStats.cc
  PlusOneBackupSelector.cc
  PortAlarm.cc
  PreparedOp.cc
  RamCloud.cc
  RawMetrics.cc
  ReplicaManager.cc
  ReplicatedSegment.cc
  RpcLevel.cc
  RpcWrapper.cc
  RpcResult.cc
  RpcTracker.cc
  Seglet.cc
  SegletAllocator.cc
  Segment.cc
  SegmentIterator.cc
  SegmentManager.cc
  ServerIdRpcWrapper.cc
  ServerList.cc
  ServerMetrics.cc
  Service.cc
  ServiceLocator.cc
  SessionAlarm.cc
  SideLog.cc
  SpinLock.cc
  Status.cc
  StringUtil.cc
  Tablet.cc
  TableEnumerator.cc
  TableStats.cc
  TabletManager.cc
  TaskQueue.cc
  TcpTransport.cc
  TestLog.cc
  ThreadId.cc
  TimeCounter.cc
  TimeTrace.cc
  TimeTraceUtil.cc
  TpcC.cc
  Transaction.cc
  TransactionManager.cc
  Transport.cc
  TransportManager.cc
  TxDecisionRecord.cc
  TxRecoveryManager.cc
  UdpDriver.cc
  UnackedRpcResults.cc
  Util.cc
  WallTime.cc
  WireFormat.cc
  WorkerManager.cc
  WorkerManagerMetrics.cc
  WorkerSession.cc
  WorkerTimer.cc
  ${PROTO_SRCS}${PROTO_HDRS})

if(INFINIBAND)
  target_sources(ramcloud
    PRIVATE
    Infiniband.cc
    InfRcTransport.cc
    InfUdDriver.cc)
endif(INFINIBAND)

target_link_libraries(ramcloud quantadb "${CMAKE_SHARED_LINKER_FLAGS}" prometheus-cpp-pull)
add_dependencies(ramcloud prometheus-build)
