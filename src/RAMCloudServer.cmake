add_library(ramcloudserver SHARED "")
if(DSSNTX)
else()
  target_sources(ramcloudserver
    PRIVATE
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
endif(DSSNTX)

link_directories(${CMAKE_CURRENT_SOURCE_DIR})

list(APPEND LIBS ramcloud ramcloudserver Message pcrecpp boost_program_options protobuf rt tbb pthread)
add_executable(server ServerMain.cc OptionParser.cc)
target_link_libraries(server ${LIBS})
