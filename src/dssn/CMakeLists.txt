add_library(dssn SHARED "")
  target_sources(dssn
    PRIVATE
    ActiveTxSet.cc
    BlockedTxSet.cc
    clhash.cc
    ClusterTimeService.cc
    CountBloomFilter.cc
    DistributedTxSet.cc
    DSSNService.cc
    DSSNServiceMonitor.cc
    HashmapKVStore.cc
    KVStore.cc
    PeerInfo.cc
    Sequencer.cc
    TxEntry.cc
    TxLog.cc
    Validator.cc
    )

install(TARGETS dssn
  PERMISSIONS OWNER_EXECUTE OWNER_READ OWNER_WRITE
              GROUP_EXECUTE GROUP_READ GROUP_WRITE
	      WORLD_EXECUTE
  LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX})
