package common

import (
	"hash/crc32"
	"time"
)

// unique global identifier for a chunk
type ChunkUsername uint64

// ChunkHandle is an alias for ChunkUsername
type ChunkHandle = ChunkUsername

// ServerID is a unique global identifier for a server
type ServerID uint64

// FileID is a unique global identifier for a file
type FileID string

// FileIndex is a position of a chunk in a file
type ChunkIndex uint64

// ChunkSize is the size of a chunk in bytes (default 64MB) GFS uses 64MB
const ChunkSize = uint64(1024 * 1024 * 64) // 64MB

// ChunkVersion tracks chunk version to detect stale replicas
type ChunkVersion uint64

// Version is an alias for ChunkVersion
type Version = ChunkVersion

// WriteID uniquely identifies a write operation (serial number assigned by primary)
type WriteID uint64

type ReplicaID struct {
	Username ChunkUsername
	Address  string
}

// ChunkServerID is a unique identifier for a chunk server
type ChunkServerID string

// Chunk represents a chunk of data
type Chunk struct {
	Username ChunkUsername
	Version  ChunkVersion
	Data     []byte
	Checksum uint32
}

// ChunkLocation represents a location of a chunk
type ChunkLocation struct {
	ServerID      ChunkServerID
	ServerAddress string
}

// ChunkMetadata is metadata for a chunk
type ChunkMetadata struct {
	Username  ChunkUsername
	FileID    FileID
	Index     ChunkIndex // position of the chunk in the file
	Version   ChunkVersion
	Size      uint64
	Locations []string // list of chunkservers
}

// ChunkData holds the metadata and raw bytes for a chunk
type ChunkData struct {
	Metadata ChunkMetadata
	Data     []byte
}

// FileMetadata is metadata for a file
type FileMetadata struct {
	ID             FileID
	Path           string
	Size           uint64
	ChunkCount     uint64
	ChunkUsernames []ChunkUsername
	CreationTime   time.Time
	LastModified   time.Time
}

// MutationType - type of mutation operation
type MutationType int

type Mutation struct {
	Type   MutationType
	Offset uint64
	Data   []byte // for write/append operations
}

const (
	MutationCreate MutationType = iota
	MutationDelete
	MutationWrite
	MutationAppend
)

// operation status
type Status int

const (
	StatusOK Status = iota
	StatusError
	StatusCorrupted
	StatusExpired
	StatusAlreadyExists
	StatusNoLease
	StatusNotFound
)

// ChunkReport includes version info for stale replica detection
type ChunkReport struct {
	Username ChunkUsername
	Version  ChunkVersion
}

// GrantVersionArgs is sent from master to chunkserver to bump the chunk version
type GrantVersionArgs struct {
	Username   ChunkUsername
	NewVersion ChunkVersion
}

// GrantVersionReply is the chunkserver's response to a version bump
type GrantVersionReply struct {
	Status Status
}

// HeartbeatRequest is sent from a chunkserver to the master
type HeartbeatRequest struct {
	Address       string
	Chunks        []ChunkUsername
	ChunkReports  []ChunkReport  // includes version for stale detection
	Capacity      uint64
	UsedSpace     uint64
	LeaseRenewals []ChunkUsername // chunks where this server is primary and wants renewal
}

// HeartbeatReply is the master's response to a heartbeat
type HeartbeatReply struct {
	Status            Status
	ChunksToDelete    []ChunkUsername
	ChunksToReplicate []ChunkUsername
	RenewedLeases     []ChunkUsername // chunks whose leases were successfully renewed
}

// Checksum computes a CRC32 checksum of data
func Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
