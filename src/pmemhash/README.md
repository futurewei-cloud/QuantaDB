# PmemHash: A Lockless CRCW Hashmap for Persistent Memory

PmemHash, part of the PelagoDB, is built and maintained by PelagoDB developers.

PmemHash features lockfree concurrent-read, concurrent-write operations
leveraging atomic instructions like CompareAndSwap. It also supports 
concurrent-read exclusive-write operation mode, where atomic instructions
are not required.

It can be configured to be lossless, where capacity overflow will result
failure. It can also be configured to be lossy, where capacity overflow
will result eviction.

To achieve good load factor, it uses N-Way (N>50) associative buckets to
store signature and persistent pointers. It also uses vectorized signature
lookup to accelerate search.

