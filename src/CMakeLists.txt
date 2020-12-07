# Copyright (c) 2020 Futurewei Technologies, Inc.
# All rights reserved.

ADD_SUBDIRECTORY(${CMAKE_CURRENT_SOURCE_DIR}/dssn)

include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudLib.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudCoord.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/RAMCloudServer.cmake)
if(DEBUG)
  include(${CMAKE_CURRENT_SOURCE_DIR}/unittest.cmake)
endif(DEBUG)

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


add_library(message SHARED "")
target_sources(message
  PRIVATE
  ${PROTO_SRCS} ${PROTO_HDRS})

if(DEBUG)
  install(TARGETS test
  RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX})
endif(DEBUG)

install(TARGETS coordinator server
  RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX})

install(TARGETS message ramcloud ramcloudcoord ramcloudserver
  PERMISSIONS OWNER_EXECUTE OWNER_READ OWNER_WRITE
              GROUP_EXECUTE GROUP_READ GROUP_WRITE
	      WORLD_EXECUTE
  LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})

