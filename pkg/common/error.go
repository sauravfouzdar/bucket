package common

import (
	"errors"
)

// Common errors
var (
	ErrFileNotFound             = errors.New("file not found")
	ErrFileExists               = errors.New("file already exists")
	ErrChunkNotFound            = errors.New("chunk not found")
	ErrNoAvailableChunkServer   = errors.New("no available chunkserver")
	ErrChunkServerAlreadyExists = errors.New("chunk server already registered")
	ErrStaleChunk               = errors.New("stale chunk")
	ErrLeaseExpired             = errors.New("lease expired")
	ErrLeaseNotFound            = errors.New("lease not found")
	ErrInvalidOffset            = errors.New("invalid offset")
	ErrInvalidArgument          = errors.New("invalid argument")
	ErrRPCFailed                = errors.New("RPC failed")
	ErrTimeout                  = errors.New("operation timeout")
	ErrChecksumMismatch         = errors.New("checksum mismatch")
	ErrInvalidChunkVersion      = errors.New("invalid chunk version")
	ErrNotPrimary               = errors.New("this server is not the primary for the chunk")
	ErrVersionMismatch          = errors.New("chunk version mismatch")
)
