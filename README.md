# Surfstore

A networked file storage application (based on Dropbox) in Go that lets users sync files to and from the cloud. 
Implements the RAFT protocol for fault-tolerance and consistent hashing for scalability.

## Basic Description

Surfstore is composed of the following two services:

1. **BlockStore:** The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. This service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

2. **MetaStore (RaftSurfstore):** The MetaStore service manages the metadata of files and the entire system. Most importantly, the MetaStore service holds the mapping of filenames to blocks. Furthermore, it should be aware of available BlockStores and map blocks to particular BlockStores.    

The source code can be found in `pkg/surfstore/` and `cmd/`. 

## Scalability: Consistent hashing

Consider a deployment of SurfStore with 1000 block store servers.  To upload a file, we’ll break it into blocks, and upload those blocks to the block store service (consisting of 1000 servers).  Consider block B_0 with hash value H_0.  On which of the 1000 block stores should B_0 be stored?  We could store it on a random block store server, but then how would we find it? (We’d have to connect to all 1000 servers looking for it…).  On the other hand, we could have a single “index server” that kept the mapping of block hash H_0 to which block store is used to store B_0.  But this single index server becomes a bottleneck.  We could use a simple hash function to map the hash value H_0 to one of the block servers, but if we ever changed the size of the set of servers, then we’d have to reload all the data, which is quite inefficient.

Instead, we implement a mapping approach based on consistent hashing.  [Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing) is a distributed hashing scheme that operates independently of the number of servers or objects in a distributed hash table by assigning them a position on an abstract circle, or hash ring. When the MetaStore server is started, the program will create a consistent hash ring in MetaStore. Since we’re providing a command line argument including each block server’s address, each block server will have a name in the format of “blockstore” + address (e.g. blockstorelocalhost:8081, blockstorelocalhost:8082, etc). We’ll hash these strings representing the servers using the same hash function as the block hashes – SHA-256.

Each time when we update a file, the program will break the file into blocks and compute hash values for each block. Then we use the consistent hashing algorithm to get a map indicating which servers the blocks belong to. Based on this map, we can upload our blocks to corresponding block servers. 

## Fault-Tolerance: RAFT protocol

The RAFT protocol is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The key idea of RAFT is to divide the consensus problem into three relatively independent subproblems: leader election, log replication, and safety. 

We implement the RAFT protocol from the paper by [Ongaro et al.](https://raft.github.io/raft.pdf), except for log compaction and membership changes. 

Each RaftSurfstoreServer will communicate with other RaftSurfstoreServers via GRPC. Each server is aware of all other possible servers (from the configuration file), and new servers do not dynamically join the cluster (although existing servers can “crash” via the Crash api). Leaders will be set through the SetLeader API call, so there are no elections.

Using the protocol, if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.  As long as a majority of the nodes are up and not in a crashed state, the clients will be able to interact with the system successfully.  When a majority of nodes are in a crashed state, clients will block and not receive a response until a majority are restored.  Any clients that interact with a non-leader will get an error message and retry to find the leader.

Here's a helpful visualization of the RAFT protocol: [RAFT visualization](http://thesecretlivesofdata.com/raft/).


## Makefile

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```

## More details

This project was developed in 3 phases as a part of the CS 224: Graduate Networked Systems course at UCSD offered in Winter 2023. More specifications about the project can be found in the documents offered by the course:

1. [Phase 1](https://docs.google.com/document/d/1-MI25UT0bwNPzByoNwOSDwY-Et5x1KxraKhMAUYyQ1Q/edit?usp=sharing)
2. [Phase 2](https://docs.google.com/document/d/1KHbEP_zS3vq4rdLe2vQ1GgkIu-6MNc8mstdh4APFdsE/edit?usp=sharing)
3. [Phase 3](https://docs.google.com/document/d/1s_Ba_ba_f11ZU08f9Yo34SiH9KjF8M3Rz2H8US0FOpI/edit?usp=sharing)