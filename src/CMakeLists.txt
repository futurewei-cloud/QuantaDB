ADD_SUBDIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/dssn)

include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudLib.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudCoord.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudServer.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/unittest.cmake)

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../gtest)


protobuf_generate_cpp(PROTO_SRCS PROTO_HDS
  CoordinatorClusterClock.proto
  CoordinatorUpdateInfo.proto
  EnumerationIterator.proto
  Histogram.proto
  Indexlet.proto
  LogMetrics.proto
  MasterRecoveryInfo.proto
  MetricList.proto
  ProtoBufTest.proto
  RecoveryPartition.proto
  ServerConfig.proto
  ServerListEntry.proto
  ServerList.proto
  ServerStatistics.proto
  SpinLockStatistics.proto
  TableConfig.proto
  TableManager.proto
  Table.proto
  Tablets.proto)


add_library(Message SHARED "")
target_sources(Message
  PRIVATE
  ${PROTO_SRCS} ${PROTO_HDRS})


  

