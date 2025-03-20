package master

import (
	"encoding/json"
	"os"
	"sync"
	"time"
	"path/filepath"

	"github.com/sauravfouzdar/pkg/common"
)

// MetadataManager manages bucket metadata
type MetadataManager struct {
	// File namespace: path -> FileID
	namespace map[string]common.FileID
	namespaceMutex sync.RWMutex

	// file metadata: FileID -> FileMetadata
	files map[common.FileID]*common.FileMetadata
	filesMutex sync.RWMutex

	// chunk metadata: ChunkUsername -> ChunkMetadata
	chunks map[common.ChunkUsername].*common.ChunkMetadata
	chunksMutex sync.RWMutex

	// Next IDs
	nextFileID common.FileID
	nextChunkUsername common.ChunkUsername
	idMutex sync.Mutex

	// checkpoint location
	checkpointDir string

}

func NewMetadataManager() *MetadataManager {
	return &MetadataManager{
		namespace: make(map[string]common.FileID),
		files: make(map[common.FileID]*common.FileMetadata),
		chunks: make(map[common.ChunkUsername]*common.ChunkMetadata),
		nextFileID: 1,
		nextChunkUsername: 1,
		checkpointDir: "./metadata",
	}
}

// SaveToDisk saves metadata to disk
func (mm *MetadataManager) SaveToDisk() error {

	// 0755 - rwxr-xr-x
	if err := os.MkdirAll(mm.checkpointDir, 0755); err != nil {
		return err
	}

	// Save namespace
	mm.namespaceMutex.RLock()
	namespaceData, err := json.Marshal(mm.namespace)
	mm.namespaceMutex.RUnlock()
	if err != nil {
		return err
	}
	// 0644 - rw-r--r--
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "namespace"), namespaceData, 0644); err != nil {
		return err
	}

	// Save files
	mm.filesMutex.RLock()
	filesData, err := json.Marshal(mm.files)
	mm.filesMutex.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "files.json"), filesData, 0644); err != nil {
		return err
	}

	// Save chunks
	mm.chunksMutex.RLock()
	chunksData, err := json.Marshal(mm.chunks)
	mm.chunksMutex.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "chunks.json"), chunksData, 0644); err != nil {
		return err
	}

	// Save next IDs
	mm.idMutex.Lock()
	ids := struct {
		NextFileID common.FileID `json:"nextFileID"`
		NextChunkUsername common.ChunkUsername `json:"nextChunkUsername"`
	}{
		NextFileID: mm.nextFileID,
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

	// check if file already exists
	if _, ok := mm.namespace[path]; ok {
		mm.namespaceMutex.RUnlock()
		return "", common.ErrFileExists
	}

	// generate new file ID
	mm.idMutex.Lock()
	fileID := common.FileID(filepath.Base(path)) + "-" + time.Now().Format("20060112-150405") + "-" +
	mm.nextFileID++
	mm.idMutex.Unlock()

	now := time.Now()
	fileMetadata := &common.FileMetadata{
		ID: fileID,
		Path: path,
		Size: 0,
		ChunkCount: 0,
		ChunkUsernames: []common.ChunkUsername{},
		CreationTime: now,
		LastModified: now,
	}

	// update namespace
	mm.filesLock.Lock()
	mm.files[fileID] = fileMetadata
	mm.filesLock.Unlock()

	return fileID, nil
}

// DeleteFile deletes a file from the namespace
func (mm *MetadataManager) DeleteFile(path string) error {
	
	// get file ID
	mm.namespaceMutex.RLock()
	fileID, ok := mm.namespace[path]
	mm.namespaceMutex.RUnlock()

	if !ok {
		return common.ErrFileNotFound
	}

	// Get chunks to delete
	mm.filesMutex.RLock()
	file, ok := mm.files[fileID]
	if !ok {
		mm.filesMutex.RUnlock()
		return common.ErrFileNotFound
	}

	ChunkUsernames := make([]common.ChunkUsername, len(file.ChunkUsernames))
	copy(ChunkUsernames, file.ChunkUsernames)
	mm.filesMutex.RUnlock()

	// delete chunks
	mm.chunksMutex.Lock()
	for _, username := range ChunkUsernames {
		delete(mm.chunks, username)
	}
	mm.chunksMutex.Unlock()

	// remove file from namespace
	mm.namespaceMutex.Lock()
	delete(mm.namespace, path)
	mm.namespaceMutex.Unlock()

	return nil
}

// GetFileMetadata returns metadata for a file
func (mm *MetadataManager) GetFileMetadata(path string) (*common.FileMetadata, error) {
	
	mm.filesMutex.RLock()
	fileID, ok := mm.namespace[path]
	mm.filesMutex.RUnlock()
	if !ok {
		return nil, common.ErrFileNotFound
	}
	
	// get file metadata
	mm.filesMutex.RLock()
	defer mm.filesMutex.RUnlock()
	file, ok := mm.files[fileID]
	if !ok {
		return nil, common.ErrFileNotFound
	}

	// return a copy
	fileCopy := *file
	return &fileCopy, nil
}

// GetChunkUsername returns the chunk username for a file and chunk index
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

// CreateChunk creates a new chunk
func (mm *MetadataManager) CreateChunk(fileID common.FileID, index common.ChunkIndex) (common.ChunkUsername, error) {
	// get file metadata
	mm.filesMutex.RLock()
	file, ok := mm.files[fileID]
	mm.filesMutex.RUnlock()

	if !ok {
		return 0, common.ErrFileNotFound
	}

	// generate new chunk username
	mm.idMutex.Lock()
	username := mm.nextChunkUsername
	mm.nextChunkUsername++
	mm.idMutex.Unlock()

	/*
		When a chunk is first created:

		- No server has been granted a lease to coordinate mutations
		- No primary replica has been designated
		- The chunk is in a "neutral" state with regard to mutations

		The relevant fields are implicitly set to their zero values:

		PrimaryReplica defaults to "" (empty string)
		LeaseExpiration defaults to time.Time{} (zero time value)
	*/

	// create chunk metadata
	chunk := &common.ChunkMetadata{
		Username: username,
		FileID: fileID,
		Index: index,
		Version: 1, 
		Size: 0,
		Locations: []string{},
		PrimaryReplica: "",
		LeaseExpiration: time.Time{},
	}

	// update chunk map
	mm.chunksMutex.Lock()
	mm.chunks[username] = chunk
	mm.chunksMutex.Unlock()

	// update file metadata
	mm.filesMutex.Lock()
	defer mm.filesMutex.Unlock()

	// check if file still exists
	file, ok := mm.files[fileID]
	if !ok {
		return 0, common.ErrFileNotFound
	}

	// ensure slice is large enough
	if int(index) >= len(file.ChunkUsernames) {
		file.ChunkUsernames = append(file.ChunkUsernames, 0)
	}

	file.ChunkUsernames[index] = username
	file.ChunkCount = uint64(len(file.ChunkUsernames))
	file.LastModified = time.Now()

	return username, nil
}

// GetChunkMetadata returns metadata for a chunk
func (mm *MetadataManager) GetChunkMetadata(username common.ChunkUsername) (*common.ChunkMetadata, error) {

	chunkMutex.RLock()
	// get chunk metadata
	chunk, ok := mm.chunks[username]
	chunkMutex.RUnlock()
	if !ok {
		return 0, common.ErrChunkNotFound
	}


	// return a copy
	chunkCopy := *chunk
	return &chunkCopy, nil
}

// GetChunkLocations returns the locations of a chunk
func (mm *MetadataManager) GetChunkLocations(username common.ChunkUsername) ([]string, error) {

	mm.chunksMutex.RLock()
	defer mm.chunksMutex.RUnlock()
	chunk, ok := mm.chunks[username]
	mm.chunksMutex.RUnlock()
	if !ok {
		return nil, common.ErrChunkNotFound
	}

	locations := make([]string, len(chunk.Locations))
	copy(locations, chunk.Locations)

	return locations, nil
}

// AddChunkLocation adds a location for a chunk
func (mm *MetadataManager) AddChunkLocation(username common.ChunkUsername, location string) {
	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	// check if location already exists
	for _, loc := range chunk.Locations {
		if loc == location {
			return nil
		}
	}

	chunk.Locations = append(chunk.Locations, location)
}

// RemoveChunkLocation removes a location for a chunk
func (mm *MetadataManager) RemoveChunkLocation(username common.ChunkUsername, location string) {

	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	// find location
	for i, loc := range chunk.Locations {
		// swap with last element and truncate
		if loc == location {
			lastIndex := len(chunk.Locations) - 1
			chunk.Locations[i] = chunk.Locations[lastIndex]
			chunk.Locations = chunk.Locations[:lastIndex]
			return
		}
	}
}

// UpdateLease updates the lease expiration for a chunk
func (mm *MetadataManager) UpdateLease(username common.ChunkUsername, primary string, expiration time.Time) error {

	mm.chunksMutex.Lock()
	defer mm.chunksMutex.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	chunk.PrimaryReplica = primary
	chunk.LeaseExpiration = expiration
	return nil
}

// UpdateChunkVersion updates the version of a chunk
func (mm *MetadataManager) UpdateChunkVersion(username common.Username) (chunk.Version, error) {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()

	chunk, ok := mm.chunks[username]
	if !ok {
		return 0, common.ErrChunkNotFound
	}

	chunk.Version++
	return chunk.Version, nil
}

// UpdateFileSize updates the size of a file
func (mm *MetadataManager) UpdateFileSize(fileId common.FileID, newSize common.Size) (common.Size, error) {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()

	file, ok := mm.chunk[fileID]
	if !ok {
		return 0, ErrFileNotFound
	}

	file.Size := newSize
	file.ModificationTime = time.Now()

	return nil
}