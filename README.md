# Bucket 
Bucket is a naive implementation of GFS distributed file system that supports basic file operations like read, write, and delete. This project is to gain better understanding of distributed storage systems.

## Architecture
![architecture](https://github.com/sauravfouzdar/bucket/blob/master/diagram.png?raw=true)

### TBD:
- [ ] Add checksum
- [ ] Add logging
- [ ] Improve error handling

## Project Structure:

```
 gfs/
├── cmd/
│   ├── master/
│   │   └── main.go          # Master server entry point
│   ├── chunkserver/
│   │   └── main.go          # Chunk server entry point
│   └── client/
│       └── main.go          # Client CLI/API entry point
│
├── pkg/
│   ├── master/
│   │   ├── server.go        # Master server implementation
│   │   ├── namespace.go     # File namespace management
│   │   ├── metadata.go      # Metadata management
│   │   ├── chunk_manager.go # Chunk allocation and management
│   │   └── lease_manager.go # Lease management for mutations
│   │
│   ├── chunkserver/
│   │   ├── chunkserver.go   # Chunk server implementation
│   │   ├── chunk_store.go   # Local chunk storage
│   │   ├── checksum.go      # Data integrity checking
│   │   └── replication.go   # Chunk replication logic
│   │
│   ├── client/
│   │   ├── client.go        # Client library
│   │   ├── cache.go         # Metadata caching
│   │   ├── reader.go        # File reading operations
│   │   └── writer.go        # File writing operations
│   │
│   ├── common/
│   │   ├── types.go         # Shared data structures
│   │   ├── constants.go     # System constants
│   │   ├── errors.go        # Custom error types
│   │   └── utils.go         # Utility functions
│   │
│   ├── rpc/
│   │   ├── master_rpc.go    # Master RPC definitions
│   │   ├── chunk_rpc.go     # Chunkserver RPC definitions
│   │   └── protocols.go     # RPC protocol definitions
│   │
│   └── network/
│       ├── transport.go     # Network transport layer
│       └── heartbeat.go     # Heartbeat mechanism
│
├── internal/
│   ├── log/
│   │   └── logger.go        # Logging utilities
│   └── config/
│       └── config.go        # Configuration management
│
├── test/
│   ├── integration/         # Integration tests
│   └── benchmark/          # Performance benchmarks
│
├── configs/
│   ├── master.yaml         # Master configuration
│   ├── chunkserver.yaml    # Chunkserver configuration
│   └── client.yaml         # Client configuration
│
├── go.mod
├── go.sum
├── Makefile
└── README.md

```

## References:
- [Paper](https://gist.github.com/nficano/d6dcb1c5c3dccbfdbc85d39d4fa16323)
