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
    HashmapKVStore.cc
    KVStore.cc
    PeerInfo.cc
    Sequencer.cc
    TxEntry.cc
    TxLog.cc
    Validator.cc
    )

target_link_libraries(dssn)
