# Bucket 
Bucket is a naive implementation of GFS distributed file system that supports basic file operations like read, write, and delete. This project is to gain better understanding of distributed storage systems.
 
## Project Structure:

```
 ├── cmd
 │   ├── master
 │   │   └── main.go         # Master node entry point
 │   └── chunkserver
 │       └── main.go         # Chunk server entry point
 ├── pkg
 │   ├── master              # Master server implementation
 │   │   ├── master.go       # Master server core
 │   │   ├── metadata.go     # Metadata management
 │   │   └── lease.go        # Chunk lease management
 │   ├── chunkserver         # Chunk server implementation
 │   │   ├── server.go       # Chunk server core
 │   │   ├── chunk.go        # Chunk management
 │   │   └── storage.go      # Local storage management
 │   ├── common              # Common types and utilities
 │   │   ├── types.go        # Shared type definitions
 │   │   ├── error.go        # Error definitions
 │   │   └── config.go       # Configuration
 │   └── client              # Client library implementation
 │       ├── client.go       # Client API
 │       ├── read.go         # Read operations
 │       └── write.go        # Write operations
 └── internal
     └── rpc                 # RPC implementation
         ├── rpc.go          # RPC utilities
         ├── master_rpc.go   # Master RPC definitions
         └── chunk_rpc.go    # Chunk server RPC definitions

```
