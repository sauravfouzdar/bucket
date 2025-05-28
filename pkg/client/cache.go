package client

import (
    "sync"
    "github.com/gfs/pkg/common"
)

// Client represents a GFS client
type Client struct {
    masterAddr common.ServerAddress
    cache      *MetadataCache
    leaseCache *LeaseCache
    mu         sync.RWMutex
}

// MetadataCache caches file and chunk metadata
type MetadataCache struct {
    mu          sync.RWMutex
    files       map[string]*CachedFile
    chunks      map[common.ChunkUsername]*CachedChunk
    maxEntries  int
    ttl         time.Duration
}

// LeaseCache caches leases for chunks
type LeaseCache struct {
	mu         sync.RWMutex
	leases     map[common.ChunkUsername]time.Time
	maxEntries int
	ttl        time.Duration
}

// CachedFile represents cached file metadata
type CachedFile struct {
    Info       common.FileInfo
    Expiration time.Time
}

// CachedChunk represents cached chunk location info
type CachedChunk struct {
    Info       common.ChunkInfo
    Expiration time.Time
}

// FileHandle represents an open file
type FileHandle struct {
    client   *Client
    filename string
    mode     FileMode
    offset   common.Offset
    metadata *common.FileInfo
}

