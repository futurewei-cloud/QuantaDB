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

cmake_minimum_required(VERSION 2.8)

# set the project name
project(QuantaDB)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)
# enable -fPIC
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# List of packages
find_package(PythonInterp 2.7 REQUIRED)
find_package(Protobuf REQUIRED)
find_library(ZOOKEEPER zookeeper)

find_program(INFINIBAND NAMES ibv_devices)

option(QDBTX "QDBTX" ON)
option(QDBTX "QDBTXRECOVERY" OFF)
option(MONITOR "MONITOR" ON)
option(DEBUG "DEBUG" ON)
option(DEBUG_OPT "DEBUG_OPT" OFF)
option(YIELD "YIELD" OFF)
option(VALGRIND "VALGRIND" OFF)

#Installation TARGET directory
set(CMAKE_INSTALL_PREFIX "${CMAKE_SOURCE_DIR}/obj.master")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}")

# List of debug flags
set(CXX_DEBUG_FLAGS "-g")
# List of application specific compilation flags
set(CXX_APP_FLAGS "")
# List of optimization flags
set(CXX_OPT_FLAGS "")

set(CMAKE_SHARED_LINKER_FLAGS "-lpcrecpp -lboost_program_options -lprotobuf -lrt -lboost_filesystem -lboost_system -lpthread -lssl -lcrypto -ltbb -ljemalloc")

if(DEBUG)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -rdynamic")
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DTESTING=1 -fno-builtin")
  if (DEBUG_OPT)
    set(CXX_OPT_FLAGS "${CXX_OPT_FLAGS} -O0")
  endif(DEBUG_OPT)
else()
  set(CXX_OPT_FLAGS "${CXX_OPT_FLAGS} -O3 -DNDEBUG")
endif(DEBUG)

if(QDBTX)
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DQDBTX")
  if(QDBTXRECOVERY)
    set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DQDBTXRECOVERY")
  endif(QDBTXRECOVERY)
endif(QDBTX)

if(MONITOR)
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DMONITOR")
endif(MONITOR)

if(INFINIBAND)
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -libverbs")
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DINFINIBAND")
endif(INFINIBAND)

#All compilation warming flags here
set(CXX_WARN_FLAGS "-Wall -Wformat=2 -Wextra -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute -Wno-non-template-friend -Woverloaded-virtual -Wcast-qual -Wcast-align -Wno-deprecated-declarations -Wall -Wfatal-errors")

#All compilation option flags here
set(CXX_OPTION_FLAGS "-fno-strict-aliasing -MD -msse4.2 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")
#set(CXX_OPTION_FLAGS "-fno-strict-aliasing -MD -msse4.2 -march=native -faligned-new")

#Aggregate of all compilation flags
set(CMAKE_CXX_FLAGS "${CXX_DEBUG_FLAGS} ${CXX_OPT_FLAGS} ${CXX_WARN_FLAGS} ${CXX_OPTION_FLAGS} ${CXX_APP_FLAGS}")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(gtest/include)
include_directories(hot/libs/hot/rowex/include)
include_directories(hot/libs/hot/commons/include)
include_directories(hot/libs/idx/content-helpers/include)
include_directories(hot/third-party/tbb/include)
include_directories(pmemhash/internal)
include_directories(pmemhash/utils)
include_directories(src)
include_directories(src/quantadb)
include_directories(prometheus-cpp/core/include)
include_directories(prometheus-cpp/build/core/include)
include_directories(prometheus-cpp/pull/include)
include_directories(prometheus-cpp/build/pull/include)


#TODO: Fix the package dependencies
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/hot/build/tbb_cmake_build/tbb_cmake_build_subdir_release)
link_directories(${CMAKE_BINARY_DIR}/prometheus/build/lib/)

ADD_SUBDIRECTORY(apps)
ADD_SUBDIRECTORY(nanobenchmarks)
ADD_SUBDIRECTORY(src)

add_custom_target(
  RpcLevelDataHdr ALL
  BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/src/RpcLevelData.h
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/genLevels.py ${CMAKE_CURRENT_BINARY_DIR}/src/RpcLevelData.h
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/WireFormat.h
  )

add_custom_target(
  RawMetricsHdr ALL
  BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/src/RawMetrics.in.h ${CMAKE_CURRENT_BINARY_DIR}/src/RawMetrics.in.cc
  COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/scripts/rawmetrics.py ${CMAKE_CURRENT_BINARY_DIR}/src/
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/WireFormat.h
  )

#add_custom_target(
#  JAVAClient ALL
#  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/bindings/java/gradlew --project-dir bindings/java
#  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
#  DEPENDS ramcloud
#)

include(ExternalProject)
ExternalProject_Add(prometheus
  PREFIX prometheus
  SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/../prometheus-cpp
  BINARY_DIR ${CMAKE_CURRENT_BINARY_DIR}/prometheus/build
  STEP_TARGETS build
  CMAKE_ARGS -D BUILD_SHARED_LIBS=ON -D CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}
  EXCLUDE_FROM_ALL TRUE
  )

install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/prometheus/build/lib/
  DESTINATION ${CMAKE_INSTALL_PREFIX}
  USE_SOURCE_PERMISSIONS)

