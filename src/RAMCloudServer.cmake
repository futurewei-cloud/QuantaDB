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

add_library(ramcloudserver SHARED
  AdminService.cc
  BackupMasterRecovery.cc
  BackupService.cc
  BackupStorage.cc
  HashObjectManager.cc
  HashTable.cc
  InMemoryStorage.cc
  LockTable.cc
  MasterService.cc
  MultiFileStorage.cc
  PriorityTaskQueue.cc
  RecoverySegmentBuilder.cc
  Server.cc)


link_directories(${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND LIBS ramcloudserver message)

add_executable(server ServerMain.cc OptionParser.cc)
target_link_libraries(server PUBLIC ${ramcloud}
                      ${LIBS})
