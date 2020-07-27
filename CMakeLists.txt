cmake_minimum_required(VERSION 2.8)

# set the project name
project(DSSN)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# List of packages
find_package(PythonInterp 2.7 REQUIRED)
find_package(Protobuf REQUIRED)

find_library(ZOOKEEPER zookeeper)
find_library(INIFIBAND verbs)


option(DSSNTX "DSSNTX" ON)
option(DEBUG "DEBUG" ON)
option(DEBUG_OPT "DEBUG_OPT" OFF)
option(YIELD "YIELD" OFF)
option(VALGRIND "VALGRIND" OFF)

# List of debug flags
set(CXX_DEBUG_FLAGS "-g")
# List of application specific compilation flags
set(CXX_APP_FLAGS "")
# List of optimization flags
set(CXX_OPT_FLAGS "")

if(DEBUG)
  set(CXX_DEBUG_FLAGS "${CXX_DEBUG_FLAGS} -rdynamic")
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DTESTING")
  if (DEBUG_OPT)
    set(CXX_OPT_FLAGS "${CXX_OPT_FLAGS} -O0")
  endif(DEBUG_OPT)
else()
  set(CXX_OPT_FLAGS "${CXX_OPT_FLAGS} -O3")
endif(DEBUG)

if(DSSNTX)
  set(CXX_APP_FLAGS "${CXX_APP_FLAGS} -DDSSNTX")
endif(DSSNTX)

#All compilation warming flags here
set(CXX_WARN_FLAGS "-Wall -Wformat=2 -Wextra -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute -Wno-non-template-friend -Woverloaded-virtual -Wcast-qual -Wcast-align -Wno-deprecated-declarations -Wall -Wfatal-errors")

#All compilation option flags here
set(CXX_OPTION_FLAGS "-fno-strict-aliasing -MD -msse4.2 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")

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
include_directories(src/dssn)

#TODO: Fix the package dependencies
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/hot/build/tbb_cmake_build/tbb_cmake_build_subdir_release)

message(${CMAKE_ROOT})

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

