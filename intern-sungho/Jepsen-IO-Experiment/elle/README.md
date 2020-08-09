# Elle Experiments 

### Consistency Level 

Name | Description | 
------------ | ------------ | 
Read uncommitted | a consistency model which prohibits **dirty writes**, where two transactions modify the same object concurrently before committing. | 
Read committed | a consistency model which strengthens read uncommitted by preventing **dirty reads**. | 
Repeatable read | a consistency model which guarantees that **any data read cannot change**, but allows **phantoms**. |
Serializability | a consistency model which  guarantees that **no new data(phantom)** can be seen by a subsequent read |
Snapshot Isolation | Each transaction appears to operate on an independent, **consistent snapshot** of the database. Its changes are visible only to that transaction until commit time, when all changes become visible atomically. | 
Strict Serializability | Operations appear to have occurred in some order, consistent with the **real-time ordering** of those operations. | 

### Reference 
- [Etcd client](https://github.com/aphyr/verschlimmbesserung)
- [Redis client](https://github.com/ptaoussanis/carmine)
- [Yugabyte client](https://github.com/yugabyte/cassaforte)

