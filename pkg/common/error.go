package common

import (
	"errors"
)


// Common errors - copied from GFS
var (
	ErrFileNotFound				= errors.New("file not found")
	ErrChunkNotFound			= errors.New("chunk not found")
	ErrNoAvailableChunkServer	= errors.New("no available chunkserver")
	ErrStaleChunk				= errors.New("stale chunk")
	ErrLeaseExpired				= errors.New("lease expired")
	ErrLeaseNotFound			= errors.New("lease not found")
	ErrInvalidOffset			= errors.New("invalid offset")
	ErrInvalidArgument			= errors.New("invalid argument")
	ErrRPCFailed				= errors.New("RPC failed")
	ErrTimeout					= errors.New("operation timeout")
)

