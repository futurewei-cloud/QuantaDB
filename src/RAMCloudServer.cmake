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

list(APPEND LIBS ramcloudserver Message)

add_executable(server ServerMain.cc OptionParser.cc)
target_link_libraries(server PUBLIC ${ramcloud}
                     ${LIBS})
