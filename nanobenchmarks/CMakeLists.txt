include_directories(${CMAKE_CURRENT_BINARY_DIR}/../src)

list(APPEND LIBS ramcloud ramcloudserver message "${CMAKE_SHARED_LINKER_FLAGS}")
  
add_executable(CleanerCompactionBenchmark CleanerCompactionBenchmark.cc)
target_link_libraries(CleanerCompactionBenchmark ${LIBS})

add_executable(Echo Echo.cc)
target_link_libraries(Echo ${LIBS})

add_executable(HashTableBenchmark HashTableBenchmark.cc)
target_link_libraries(HashTableBenchmark ${LIBS})

add_executable(HOTKVBenchmark HOTKVBenchmark.cc)
target_link_libraries(HOTKVBenchmark ${LIBS})

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
  HOTKVBenchmark LogCleanerBenchmark MigrateTabletBenchmark
  HashObjectManagerBenchmark Perf RecoverSegmentBenchmark
RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/nanobenchmarks)
