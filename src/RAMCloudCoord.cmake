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

add_library(ramcloudcoord SHARED
  ClientLeaseAuthority.cc
  CoordinatorClusterClock.cc
  CoordinatorServerList.cc
  CoordinatorService.cc
  CoordinatorUpdateManager.cc
  MasterRecoveryManager.cc
  MockExternalStorage.cc
  Tablet.cc
  TableManager.cc
  Recovery.cc
  RuntimeOptions.cc)

link_directories(${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND LIBS ramcloud ramcloudcoord ramcloudserver message)

add_executable(coordinator CoordinatorMain.cc)
target_link_libraries(coordinator PUBLIC ${ramcloud}
                      ${LIBS})
