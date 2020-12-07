# Copyright (c) 2020 Futurewei Technologies, Inc.
# All rights reserved.

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
