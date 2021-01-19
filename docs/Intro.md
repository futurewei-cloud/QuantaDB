![quantumentanglement](media/5c448181bd1398fb94924f9c7c9d6ac0.jpeg)

Jurik Peter/Shutterstock

QuantaDB

Innovative Computing and Data Lab \| Futurewei Technologies, Inc. \| Dec 2020  


# What is QuantaDB?

QuantaDB is a cloud-scale distributed transactional engine. It is
designed for applications that need low-latency transaction to a large distributed
datastore, ranging from rack-scale deployment to geo-distributed deployment.
It offers the following properties:

-   Fully distributed, cloud scale: QuantaDB uses Distributed Serial Safety Net (DSSN), a
    concurrency control and commit protocol, to support concurrent transactions
    with high throughput and low abort rates. The key components like sequencers and 
    validators, and storage nodes are fully distributed.

-   Serializability isolation transactions: QuantaDB offers the highest level of
    isolation among transactions.

-   Durability as an option: QuantaDB comes with a key-value store implemented
    as a hash table in DRAM/Persistent Memory. When it is integrated with a 
    durable key-value store, the hash table based key-value store becomes a metadata cache. 

-   Compute Storage disaggregation: QuantaDB employs disaggregated compute storage
    model, which enables deterministic commit, fast local replica read as well as fast failover.

-   Low latency: QuantaDB uses DSSN to reduce the number of rounds of network
    message exchanges to commit a transaction, which reduces the latency for multi-shard
    transactions. For single-shard transactions, QuantaDB's DRAM/PMEM-based
    key-value store provides super low latency response. QuantaDB also minimize
    the serial execution window in validtor to maximize validator's throughput.

-   Easy deployment: QuantaDB runs on commodity servers with the Linux operating
    system. Currently, it is integrated with the open source project RAMCloud to
    take advantage of its inter-process communications infrastructure and test
    suites. It can be easily integrated into other frameworks. 

-   Future proof: Even though QuantaDB is compatible with commodity hardware, it can
    fully exploit more precise (nanoseconds) and more accurate (100 nanoseconds) 
    global clock, and low latency RDMA network. The storage abstraction of QuantaDB 
    also can exploit the potential of persistent memory.

The heart of the project is that we took the concept of Serial Safety Net (SSN), which is 
meant for a single-node system, designed an efficient distributed certifier, running across 
multiple nodes and supporting multi-shard transactions. We named the new protocol as distributed
SSN (DSSN). Specifically, our protocol enables the associated nodes involved
in the same transaction to make a consistent commit or abort decision
individually and indepedently after one round of exchange on local SSN meta information such as &eta; and &pi;. 

In quantum physics, quantum entanglement refers to the
phenomenon in which the quantum states of multiple particles have to be
described with reference to each other. Any measurement of a particle's properties 
will perfectly correlated to the correspondingly entangled particle, even though the
individual particles are separated by a large distance. That is kind of like the
characteristics of our protocol that enables a transactional distributed
key-value store, where the outcome of a transaction can be deterministically computed 
by the participating nodes. Therefore, we named our project QuantaDB.

