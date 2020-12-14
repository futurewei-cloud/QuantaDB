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

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../src)

list(APPEND LIBS ramcloud ramcloudserver message "${CMAKE_SHARED_LINKER_FLAGS}")
  
add_executable(CleanerCompactionBenchmark CleanerCompactionBenchmark.cc)
target_link_libraries(CleanerCompactionBenchmark ${LIBS})

add_executable(Echo Echo.cc)
target_link_libraries(Echo ${LIBS})

add_executable(HashTableBenchmark HashTableBenchmark.cc)
target_link_libraries(HashTableBenchmark ${LIBS})

add_executable(LogCleanerBenchmark LogCleanerBenchmark.cc)
target_link_libraries(LogCleanerBenchmark ${LIBS})


add_executable(MigrateTabletBenchmark MigrateTabletBenchmark.cc)
target_link_libraries(MigrateTabletBenchmark ${LIBS})

add_executable(HashObjectManagerBenchmark HashObjectManagerBenchmark.cc)
target_link_libraries(HashObjectManagerBenchmark ${LIBS})

add_executable(Perf Perf.cc PerfHelper.cc)
target_link_libraries(Perf ${LIBS})

add_executable(RecoverSegmentBenchmark RecoverSegmentBenchmark.cc)
target_link_libraries(RecoverSegmentBenchmark ${LIBS})

Message("${INSTALL_DIR}")
install(TARGETS CleanerCompactionBenchmark Echo HashTableBenchmark
  LogCleanerBenchmark MigrateTabletBenchmark
  HashObjectManagerBenchmark Perf RecoverSegmentBenchmark
RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/nanobenchmarks)
