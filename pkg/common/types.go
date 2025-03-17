package common

import (
	"time"
)


// unique global identifier for a chunk
type ChunkUsername uint64

// FileID is a unique global identifier for a file
type FileID string

// ChunkIndex is a position of a chunk in a file
type ChunkIndex uint64

// ChunkSize is the size of a chunk in bytes (default 16MB) GFS uses 64MB
const ChunkSize = 1024 * 1024 * 16 // 16MB

// to check version of a chunk to detect stale replicas 
type ChunkVersion uint64

type ReplicaID struct {
	Username	ChunkUsername
	Address		string
}

// ChunkMetadata is metadata for a chunk 
type ChunkMetadata struct {
	Username		ChunkUsername
	FileID			FileID
	Index			ChunkIndex // position of the chunk in the file 
	Version			ChunkVersion
	Size			uint64
	Checksum		uint64
	Locations		[]string // list of chunkservers
	PrimaryReplica	string
	LeaseExpiration	time.Time
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

// Mutation represents write operation
type Mutation struct {
	Type		MutationType // create, append, delete
	Offset		uint64
	Data		[]byte
	Timestamp	time.Time
}

// MutationType - type of mutation operation
type MutationType int

const (
	MutationWrite MutationType = iota
	MutationDelete
	MutationAppend
)

// operation status
type Status int

const (
	StatusOK Status = iota
	StatusError
	StatusStaleChunk
	StatusNoLease
	StatusNotFound
)
