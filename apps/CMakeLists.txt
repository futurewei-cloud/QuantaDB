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

link_directories(${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND LIBS ramcloud message "${CMAKE_SHARED_LINKER_FLAGS}")

add_executable(backuprecovery BackupRecoveryMain.cc)
target_link_libraries(backuprecovery ${LIBS})

add_executable(client ClientMain.cc)
target_link_libraries(client ${LIBS})

add_executable(ClusterPerf ClusterPerf.cc)
target_link_libraries(ClusterPerf ${LIBS})

add_executable(CoordinatorCrashRecovery CoordinatorCrashRecovery.cc)
target_link_libraries(CoordinatorCrashRecovery ${LIBS})

add_executable(ensureServers EnsureServers.cc)
target_link_libraries(ensureServers ${LIBS})

add_executable(migrateTablet MigrateTabletMain.cc)
target_link_libraries(migrateTablet ${LIBS})

add_executable(recovery RecoveryMain.cc)
target_link_libraries(recovery ${LIBS})

add_executable(dumpPerfStats DumpPerfStats.cc)
target_link_libraries(dumpPerfStats ${LIBS})

install(TARGETS backuprecovery client ClusterPerf
  CoordinatorCrashRecovery ensureServers migrateTablet
  recovery dumpPerfStats
RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/apps)
