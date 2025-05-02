package client

// GetCachedFileMetadata gets cached file metadata
func (mc *MetadataCache) GetCachedFileMetadata(path string) (*common.FileMetadata, []common.ChunkHandle, bool)

// CacheFileMetadata caches file metadata
func (mc *MetadataCache) CacheFileMetadata(path string, metadata *common.FileMetadata, chunkHandles []common.ChunkHandle)

// InvalidateFileMetadata invalidates cached file metadata
func (mc *MetadataCache) InvalidateFileMetadata(path string)

// GetCachedChunkLocations gets cached chunk locations
func (lc *LocationCache) GetCachedChunkLocations(handle common.ChunkHandle) ([]common.ChunkLocation, bool)

// CacheChunkLocations caches chunk locations
func (lc *LocationCache) CacheChunkLocations(handle common.ChunkHandle, locations []common.ChunkLocation)

// InvalidateChunkLocations invalidates cached chunk locations
func (lc *LocationCache) InvalidateChunkLocations(handle common.ChunkHandle)