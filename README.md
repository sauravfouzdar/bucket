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

## Requirements

- Go 1.23+

## Build

Build all binaries into `bin/`:

```bash
go build -o bin/master      ./cmd/master
go build -o bin/chunkserver ./cmd/chunkserver
go build -o bin/client      ./cmd/client
```

Or verify everything compiles without producing binaries:

```bash
go build ./...
```

## Run

Start each component in its own terminal, in this order.

### 1. Master

```bash
./bin/master \
  --addr        localhost:8000 \
  --heartbeat   5              \
  --lease       60             \
  --replica     3              \
  --checkpoint  300
```

| Flag | Default | Description |
|---|---|---|
| `--addr` | `localhost:8000` | Address the master listens on |
| `--heartbeat` | `5` | Heartbeat timeout (seconds) |
| `--lease` | `60` | Chunk lease duration (seconds) |
| `--replica` | `3` | Default replication factor |
| `--checkpoint` | `300` | Metadata checkpoint interval (seconds) |

### 2. Chunk servers

Run one or more, each with a unique address and storage directory:

```bash
# Server 1
./bin/chunkserver \
  --addr   localhost:8001 \
  --master localhost:8000 \
  --root   /tmp/chunks1

# Server 2
./bin/chunkserver \
  --addr   localhost:8002 \
  --master localhost:8000 \
  --root   /tmp/chunks2
```

| Flag | Default | Description |
|---|---|---|
| `--addr` | `localhost:8001` | Address this chunk server listens on |
| `--master` | `localhost:8000` | Master server address |
| `--root` | `/tmp/chunks` | Directory for chunk data |
| `--heartbeat` | `5` | Heartbeat interval (seconds) |
| `--max-chunks` | `100` | Maximum chunks to store |

### 3. Client (interactive shell)

```bash
./bin/client --master localhost:8000
```

Commands available in the shell:

```
create <path>                    Create a new file
ls     <path>                    List directory contents
mkdir  <path>                    Create a directory
write  <path> <offset> <data>    Write data to a file
read   <path> <offset> <length>  Read data from a file
rm     <path>                    Remove a file
help                             Show this list
exit                             Quit
```

## Quick start (three terminals)

```bash
# Terminal 1 — master
go run ./cmd/master

# Terminal 2 — chunk server
go run ./cmd/chunkserver --addr localhost:8001 --root /tmp/chunks1

# Terminal 3 — client
go run ./cmd/client
```

## Example session

- go run ./cmd/master/main.go --addr localhost:8000



## Tests

```bash
go test ./...
```

## References:
- [Paper](https://gist.github.com/nficano/d6dcb1c5c3dccbfdbc85d39d4fa16323)
