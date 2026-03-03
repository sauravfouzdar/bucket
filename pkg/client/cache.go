package client

import (
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// CachedMetadata holds a cached file metadata entry
type CachedMetadata struct {
	Metadata       common.FileMetadata
	ChunkHandles   []common.ChunkHandle
	ExpirationTime time.Time
}

// CachedLocation holds cached chunk location info
type CachedLocation struct {
	Locations      []common.ChunkLocation
	ExpirationTime time.Time
}

// MetadataCache caches file metadata keyed by path
type MetadataCache struct {
	entries    map[string]*CachedMetadata
	mu         sync.RWMutex
	expiration time.Duration
}

// LocationCache caches chunk locations keyed by ChunkHandle
type LocationCache struct {
	entries    map[common.ChunkHandle]*CachedLocation
	mu         sync.RWMutex
	expiration time.Duration
}

func (mc *MetadataCache) get(path string) (*CachedMetadata, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	entry, ok := mc.entries[path]
	if !ok || time.Now().After(entry.ExpirationTime) {
		return nil, false
	}
	return entry, true
}

func (mc *MetadataCache) set(path string, meta common.FileMetadata, handles []common.ChunkHandle) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.entries[path] = &CachedMetadata{
		Metadata:       meta,
		ChunkHandles:   handles,
		ExpirationTime: time.Now().Add(mc.expiration),
	}
}

func (lc *LocationCache) get(handle common.ChunkHandle) (*CachedLocation, bool) {
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	entry, ok := lc.entries[handle]
	if !ok || time.Now().After(entry.ExpirationTime) {
		return nil, false
	}
	return entry, true
}

func (lc *LocationCache) set(handle common.ChunkHandle, locations []common.ChunkLocation) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.entries[handle] = &CachedLocation{
		Locations:      locations,
		ExpirationTime: time.Now().Add(lc.expiration),
	}
}
