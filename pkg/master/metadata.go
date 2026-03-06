package master

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// MetadataManager manages file and chunk metadata
type MetadataManager struct {
	namespace      map[string]common.FileID
	namespaceMutex sync.RWMutex

	files      map[common.FileID]*common.FileMetadata
	filesMutex sync.RWMutex

	chunks      map[common.ChunkUsername]*common.ChunkMetadata
	chunksMutex sync.RWMutex

	nextFileID        int
	nextChunkUsername common.ChunkUsername
	idMutex           sync.Mutex

	checkpointDir string
}

func NewMetadataManager() *MetadataManager {
	return &MetadataManager{
		namespace:     make(map[string]common.FileID),
		files:         make(map[common.FileID]*common.FileMetadata),
		chunks:        make(map[common.ChunkUsername]*common.ChunkMetadata),
		nextFileID:    1,
		nextChunkUsername: 1,
		checkpointDir: "./metadata",
	}
}

// LoadFromDisk loads metadata from disk during recovery
func (mm *MetadataManager) LoadFromDisk() error {
	if err := os.MkdirAll(mm.checkpointDir, 0755); err != nil {
		return err
	}

	namespaceFile := filepath.Join(mm.checkpointDir, "namespace.json")
	if data, err := os.ReadFile(namespaceFile); err == nil {
		if err := json.Unmarshal(data, &mm.namespace); err != nil {
			return err
		}
	}

	filesFile := filepath.Join(mm.checkpointDir, "files.json")
	if data, err := os.ReadFile(filesFile); err == nil {
		if err := json.Unmarshal(data, &mm.files); err != nil {
			return err
		}
	}

	chunksFile := filepath.Join(mm.checkpointDir, "chunks.json")
	if data, err := os.ReadFile(chunksFile); err == nil {
		if err := json.Unmarshal(data, &mm.chunks); err != nil {
			return err
		}
	}

	idsFile := filepath.Join(mm.checkpointDir, "ids.json")
	if data, err := os.ReadFile(idsFile); err == nil {
		var ids struct {
			NextChunkUsername common.ChunkUsername `json:"next_chunk_username"`
			NextFileID        int                  `json:"next_file_id"`
		}
		if err := json.Unmarshal(data, &ids); err != nil {
			return err
		}
		mm.nextChunkUsername = ids.NextChunkUsername
		mm.nextFileID = ids.NextFileID
	}

	return nil
}

// SaveToDisk saves metadata to disk
func (mm *MetadataManager) SaveToDisk() error {
	if err := os.MkdirAll(mm.checkpointDir, 0755); err != nil {
		return err
	}

	mm.namespaceMutex.RLock()
	namespaceData, err := json.Marshal(mm.namespace)
	mm.namespaceMutex.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "namespace.json"), namespaceData, 0644); err != nil {
		return err
	}

	mm.filesMutex.RLock()
	filesData, err := json.Marshal(mm.files)
	mm.filesMutex.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "files.json"), filesData, 0644); err != nil {
		return err
	}

	mm.chunksMutex.RLock()
	chunksData, err := json.Marshal(mm.chunks)
	mm.chunksMutex.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "chunks.json"), chunksData, 0644); err != nil {
		return err
	}

	mm.idMutex.Lock()
	ids := struct {
		NextFileID        int                  `json:"next_file_id"`
		NextChunkUsername common.ChunkUsername `json:"next_chunk_username"`
	}{
		NextFileID:        mm.nextFileID,
		NextChunkUsername: mm.nextChunkUsername,
	}
	mm.idMutex.Unlock()

	idsData, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(mm.checkpointDir, "ids.json"), idsData, 0644)
}

// CreateFile creates a new file in the namespace
func (mm *MetadataManager) CreateFile(path string) (common.FileID, error) {
	mm.namespaceMutex.RLock()
	_, exists := mm.namespace[path]
	mm.namespaceMutex.RUnlock()
	if exists {
		return "", common.ErrFileExists
	}

	mm.idMutex.Lock()
	fileID := common.FileID(fmt.Sprintf("%s-%s-%d", filepath.Base(path), time.Now().Format("20060102-150405"), mm.nextFileID))
	mm.nextFileID++
	mm.idMutex.Unlock()

	now := time.Now()
	fileMetadata := &common.FileMetadata{
		ID:             fileID,
		Path:           path,
		Size:           0,
		ChunkCount:     0,
		ChunkUsernames: []common.ChunkUsername{},
		CreationTime:   now,
		LastModified:   now,
	}

	mm.filesMutex.Lock()
	mm.files[fileID] = fileMetadata
	mm.filesMutex.Unlock()

	mm.namespaceMutex.Lock()
	mm.namespace[path] = fileID
	mm.namespaceMutex.Unlock()

	return fileID, nil
}

// DeleteFile removes a file and its chunk metadata
func (mm *MetadataManager) DeleteFile(path string) error {
	mm.namespaceMutex.RLock()
	fileID, ok := mm.namespace[path]
	mm.namespaceMutex.RUnlock()
	if !ok {
		return common.ErrFileNotFound
	}

	mm.filesMutex.RLock()
	file, ok := mm.files[fileID]
	if !ok {
		mm.filesMutex.RUnlock()
		return common.ErrFileNotFound
	}
	chunkUsernames := make([]common.ChunkUsername, len(file.ChunkUsernames))
	copy(chunkUsernames, file.ChunkUsernames)
	mm.filesMutex.RUnlock()

	mm.chunksMutex.Lock()
	for _, username := range chunkUsernames {
		delete(mm.chunks, username)
	}
	mm.chunksMutex.Unlock()

	mm.filesMutex.Lock()
	delete(mm.files, fileID)
	mm.filesMutex.Unlock()

	mm.namespaceMutex.Lock()
	delete(mm.namespace, path)
	mm.namespaceMutex.Unlock()

	return nil
}

// GetFileMetadata returns a copy of file metadata
func (mm *MetadataManager) GetFileMetadata(path string) (*common.FileMetadata, error) {
	mm.namespaceMutex.RLock()
	fileID, ok := mm.namespace[path]
	mm.namespaceMutex.RUnlock()
	if !ok {
		return nil, common.ErrFileNotFound
	}

	mm.filesMutex.RLock()
	defer mm.filesMutex.RUnlock()
	file, ok := mm.files[fileID]
	if !ok {
		return nil, common.ErrFileNotFound
	}

	fileCopy := *file
	return &fileCopy, nil
}

// GetChunkUsername returns the chunk username for a given file and chunk index
func (mm *MetadataManager) GetChunkUsername(fileID common.FileID, index common.ChunkIndex) (common.ChunkUsername, error) {
	mm.filesMutex.RLock()
	file, ok := mm.files[fileID]
	mm.filesMutex.RUnlock()
	if !ok {
		return 0, common.ErrFileNotFound
	}

	if int(index) >= len(file.ChunkUsernames) {
		return 0, common.ErrChunkNotFound
	}

	return file.ChunkUsernames[index], nil
}

// CreateChunk allocates a new chunk for a file
func (mm *MetadataManager) CreateChunk(fileID common.FileID, index common.ChunkIndex) (common.ChunkUsername, error) {
	mm.filesMutex.RLock()
	_, ok := mm.files[fileID]
	mm.filesMutex.RUnlock()
	if !ok {
		return 0, common.ErrFileNotFound
	}

	mm.idMutex.Lock()
	username := mm.nextChunkUsername
	mm.nextChunkUsername++
	mm.idMutex.Unlock()

	chunk := &common.ChunkMetadata{
		Username:  username,
		FileID:    fileID,
		Index:     index,
		Version:   1,
		Size:      0,
		Locations: []string{},
	}

	mm.chunksMutex.Lock()
	mm.chunks[username] = chunk
	mm.chunksMutex.Unlock()

	mm.filesMutex.Lock()
	defer mm.filesMutex.Unlock()

	file, ok := mm.files[fileID]
	if !ok {
		return 0, common.ErrFileNotFound
	}

	if int(index) >= len(file.ChunkUsernames) {
		file.ChunkUsernames = append(file.ChunkUsernames, username)
	} else {
		file.ChunkUsernames[index] = username
	}
	file.ChunkCount = uint64(len(file.ChunkUsernames))
	file.LastModified = time.Now()

	return username, nil
}

// GetChunkMetadata returns a copy of chunk metadata
func (mm *MetadataManager) GetChunkMetadata(username common.ChunkUsername) (*common.ChunkMetadata, error) {
	mm.chunksMutex.RLock()
	chunk, ok := mm.chunks[username]
	mm.chunksMutex.RUnlock()
	if !ok {
		return nil, common.ErrChunkNotFound
	}
	chunkCopy := *chunk
	return &chunkCopy, nil
}

// GetChunkLocations returns the server addresses for a chunk
func (mm *MetadataManager) GetChunkLocations(username common.ChunkUsername) ([]string, error) {
	mm.chunksMutex.RLock()
	defer mm.chunksMutex.RUnlock()
	chunk, ok := mm.chunks[username]
	if !ok {
		return nil, common.ErrChunkNotFound
	}
	locations := make([]string, len(chunk.Locations))
	copy(locations, chunk.Locations)
	return locations, nil
}

// AddChunkLocation adds a server address to a chunk's location list
func (mm *MetadataManager) AddChunkLocation(username common.ChunkUsername, location string) error {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	for _, loc := range chunk.Locations {
		if loc == location {
			return nil
		}
	}
	chunk.Locations = append(chunk.Locations, location)
	return nil
}

// RemoveChunkLocation removes a server address from a chunk's location list
func (mm *MetadataManager) RemoveChunkLocation(username common.ChunkUsername, location string) error {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	for i, loc := range chunk.Locations {
		if loc == location {
			last := len(chunk.Locations) - 1
			chunk.Locations[i] = chunk.Locations[last]
			chunk.Locations = chunk.Locations[:last]
			return nil
		}
	}
	return nil
}

// UpdateLease records the primary and expiration for a chunk lease
func (mm *MetadataManager) UpdateLease(username common.ChunkUsername, primary string, expiration time.Time) error {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	_, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}
	return nil
}

// UpdateChunkVersion increments and returns the chunk's version
func (mm *MetadataManager) UpdateChunkVersion(username common.ChunkUsername) (common.ChunkVersion, error) {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return 0, common.ErrChunkNotFound
	}
	chunk.Version++
	return chunk.Version, nil
}

// UpdateFileSize updates the size of a file
func (mm *MetadataManager) UpdateFileSize(fileID common.FileID, newSize uint64) error {
	mm.filesMutex.Lock()
	defer mm.filesMutex.Unlock()

	file, ok := mm.files[fileID]
	if !ok {
		return common.ErrFileNotFound
	}
	file.Size = newSize
	file.LastModified = time.Now()
	return nil
}

// MarkChunkUnavailable removes all location info for a chunk
func (mm *MetadataManager) MarkChunkUnavailable(username common.ChunkUsername) {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()
	if chunk, ok := mm.chunks[username]; ok {
		chunk.Locations = nil
	}
}

// applyCreateFile inserts a file with a pre-determined FileID directly into
// the metadata maps. Used during WAL replay to avoid re-generating IDs.
func (mm *MetadataManager) applyCreateFile(path string, fileID common.FileID) error {
	mm.namespaceMutex.Lock()
	defer mm.namespaceMutex.Unlock()

	if _, exists := mm.namespace[path]; exists {
		return nil // already present — idempotent
	}

	now := time.Now()
	mm.files[fileID] = &common.FileMetadata{
		ID:             fileID,
		Path:           path,
		Size:           0,
		ChunkCount:     0,
		ChunkUsernames: []common.ChunkUsername{},
		CreationTime:   now,
		LastModified:   now,
	}
	mm.namespace[path] = fileID

	// Bump the counter past the numeric suffix embedded in the FileID
	// (format: "name-YYYYMMDD-HHMMSS-N").
	parts := strings.Split(string(fileID), "-")
	if n, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
		mm.idMutex.Lock()
		if n >= mm.nextFileID {
			mm.nextFileID = n + 1
		}
		mm.idMutex.Unlock()
	}
	return nil
}

// applyCreateChunk inserts a chunk with a pre-determined ChunkUsername directly
// into the metadata maps. Used during WAL replay.
func (mm *MetadataManager) applyCreateChunk(fileID common.FileID, index common.ChunkIndex, username common.ChunkUsername) error {
	mm.chunksMutex.Lock()
	if _, exists := mm.chunks[username]; exists {
		mm.chunksMutex.Unlock()
		return nil // already present — idempotent
	}
	mm.chunks[username] = &common.ChunkMetadata{
		Username:  username,
		FileID:    fileID,
		Index:     index,
		Version:   1,
		Size:      0,
		Locations: []string{},
	}
	mm.chunksMutex.Unlock()

	mm.filesMutex.Lock()
	if file, ok := mm.files[fileID]; ok {
		if int(index) >= len(file.ChunkUsernames) {
			file.ChunkUsernames = append(file.ChunkUsernames, username)
		} else {
			file.ChunkUsernames[index] = username
		}
		file.ChunkCount = uint64(len(file.ChunkUsernames))
	}
	mm.filesMutex.Unlock()

	// Bump nextChunkUsername past this replayed value.
	mm.idMutex.Lock()
	if username >= mm.nextChunkUsername {
		mm.nextChunkUsername = username + 1
	}
	mm.idMutex.Unlock()
	return nil
}
