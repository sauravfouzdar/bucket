package common

import (
	"time"
	//"hash/crc32"
)


// unique global identifier for a chunk
type ChunkUsername uint64

// FileID is a unique global identifier for a file
type FileID uint64

// ChunkIndex is a position of a chunk in a file
type ChunkIndex uint64

// ChunkSize is the size of a chunk in bytes (default 16MB) GFS uses 64MB
const ChunkSize = 1024 * 1024 * 64 // 64MB

// to check version of a chunk to detect stale replicas 
type ChunkVersion uint64

type ReplicaID struct {
	Username	ChunkUsername
	Address		string
}
// ChunkServerID is a unique identifier for a chunk server
type ChunkServerID string

// Chunk represents a chunk of data
type Chunk struct {
	Username	ChunkUsername
	Version		ChunkVersion
	Data		[]byte
	Checksum	uint32
}

// ChunkLocation represents a location of a chunk
type ChunkLocation struct {
	ServerID		ChunkServerID
	ServerAddress	string
}

// ChunkMetadata is metadata for a chunk 
type ChunkMetadata struct {
	Username		ChunkUsername
	FileID			FileID
	Index			ChunkIndex // position of the chunk in the file 
	Version			ChunkVersion
	Size			uint64
	Locations		[]string // list of chunkservers
}

// FileMetadata is metadata for a file
type FileMetadata struct {
	ID				FileID
	Path			string
	Size			uint64
	ChunkCount		uint64
	ChunkUsernames	[]ChunkUsername
	CreationTime	time.Time
	LastModified	time.Time
}

// // Mutation represents write operation
// type Mutation struct {
// 	Type		MutationType // create, append, delete
// 	Offset		uint64
// 	Data		[]byte
// 	Timestamp	time.Time
// }

// MutationType - type of mutation operation
type MutationType int

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

