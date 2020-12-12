# DSSN Distributed Serial Safety Net

[TBA]: Add reference to SSN here.

# Design

Validator [TBA]
Sequencer [TBA]
DependMatrix holds any cross shard transactions can not be dispatched right away, either due to dispatch capacity limitation or
due to dependency conflict with older transactions. DependMatrix helps picking the next ready transaction faster by keeping a
dependecy chain, avoid unnecessary repeated checks.
ActiveTXFilter [TBA]
