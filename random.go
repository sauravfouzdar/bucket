// GFS Implementation in Go
// This is a simplified implementation of the Google File System (GFS) architecture
// based on the paper: https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf

// Project Structure:
// ├── cmd
// │   ├── master
// │   │   └── main.go         # Master node entry point
// │   └── chunkserver
// │       └── main.go         # Chunk server entry point
// ├── pkg
// │   ├── master              # Master server implementation
// │   │   ├── master.go       # Master server core
// │   │   ├── metadata.go     # Metadata management
// │   │   └── lease.go        # Chunk lease management
// │   ├── chunkserver         # Chunk server implementation
// │   │   ├── server.go       # Chunk server core
// │   │   ├── chunk.go        # Chunk management
// │   │   └── storage.go      # Local storage management
// │   ├── common              # Common types and utilities
// │   │   ├── types.go        # Shared type definitions
// │   │   ├── error.go        # Error definitions
// │   │   └── config.go       # Configuration
// │   └── client              # Client library implementation
// │       ├── client.go       # Client API
// │       ├── read.go         # Read operations
// │       └── write.go        # Write operations
// └── internal
//     └── rpc                 # RPC implementation
//         ├── rpc.go          # RPC utilities
//         ├── master_rpc.go   # Master RPC definitions
//         └── chunk_rpc.go    # Chunk server RPC definitions
// Let's implement each file in modular fashion below:

// --------------------- pkg/common/types.go ---------------------

package common

import (
	"time"
)

// ChunkHandle is a 64-bit globally unique ID for a chunk
type ChunkHandle uint64

// FileID is a unique identifier for a file
type FileID string

// ChunkIndex is the position of a chunk within a file
type ChunkIndex uint64

// ChunkSize is the default size of a chunk (64MB in GFS)
const ChunkSize = 64 * 1024 * 1024 // 64MB

// ChunkVersion tracks the version of a chunk to detect stale replicas
type ChunkVersion uint64

// ReplicaID identifies a specific chunk replica
type ReplicaID struct {
	Handle  ChunkHandle
	Address string
}

// ChunkMetadata contains metadata for a chunk
type ChunkMetadata struct {
	Handle         ChunkHandle
	FileID         FileID
	Index          ChunkIndex
	Version        ChunkVersion
	Size           int64
	Checksum       uint32
	Locations      []string // IP:Port of chunkservers
	PrimaryReplica string   // Current primary if lease exists
	LeaseExpiration time.Time
}

// FileMetadata contains metadata for a file
type FileMetadata struct {
	ID             FileID
	Path           string
	Size           int64
	ChunkCount     int
	ChunkHandles   []ChunkHandle
	CreationTime   time.Time
	ModificationTime time.Time
}

// Mutation represents a write operation
type Mutation struct {
	Type      MutationType
	Offset    int64
	Data      []byte
	Timestamp time.Time
}

// MutationType identifies the type of mutation
type MutationType int

const (
	MutationWrite MutationType = iota
	MutationAppend
	MutationDelete
)

// Operation status
type Status int

const (
	StatusOK Status = iota
	StatusError
	StatusStaleChunk
	StatusNoLease
	StatusNotFound
)

// --------------------- pkg/common/error.go ---------------------

package common

import (
	"errors"
)

// Common errors in GFS
var (
	ErrFileNotFound      = errors.New("file not found")
	ErrChunkNotFound     = errors.New("chunk not found")
	ErrNoAvailableServer = errors.New("no available chunkserver")
	ErrStaleChunk        = errors.New("stale chunk")
	ErrLeaseExpired      = errors.New("lease expired")
	ErrInvalidOffset     = errors.New("invalid offset")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrRPCFailed         = errors.New("RPC failed")
	ErrTimeout           = errors.New("operation timeout")
)

// --------------------- pkg/common/config.go ---------------------

package common

// Configuration for GFS components
type MasterConfig struct {
	Address            string
	HeartbeatInterval  int // seconds
	ChunkReplicaNum    int // default number of replicas for each chunk
	LeaseTime          int // seconds
	CheckpointInterval int // seconds
}

type ChunkServerConfig struct {
	Address        string
	MasterAddress  string
	StorageRoot    string
	HeartbeatInterval int // seconds
	MaxChunks      int
}

type ClientConfig struct {
	MasterAddress string
	CacheTimeout  int // seconds
}

// Default configurations
var (
	DefaultMasterConfig = MasterConfig{
		Address:            "localhost:8000",
		HeartbeatInterval:  5,
		ChunkReplicaNum:    3,
		LeaseTime:          60,
		CheckpointInterval: 300,
	}

	DefaultChunkServerConfig = ChunkServerConfig{
		MasterAddress:     "localhost:8000",
		HeartbeatInterval: 5,
		MaxChunks:         1000,
	}

	DefaultClientConfig = ClientConfig{
		MasterAddress: "localhost:8000",
		CacheTimeout:  60,
	}
)

// --------------------- pkg/master/master.go ---------------------

package master

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"gfs/internal/rpc"
	"gfs/pkg/common"
)

// Master manages the entire GFS cluster
type Master struct {
	config common.MasterConfig
	
	// Metadata management
	metadata *MetadataManager
	
	// Lease management
	leaseManager *LeaseManager
	
	// Chunk server management
	chunkServers     map[string]*ChunkServerInfo
	chunkServerMutex sync.RWMutex
	
	// Server state
	isRunning bool
	shutdown  chan struct{}
	
	// RPC server
	rpcServer *rpc.Server
}

// ChunkServerInfo contains information about a chunk server
type ChunkServerInfo struct {
	Address     string
	LastHeartbeat time.Time
	Chunks      []common.ChunkHandle
	Available   bool
	Capacity    int64
	Used        int64
}

// NewMaster creates a new master server
func NewMaster(config common.MasterConfig) *Master {
	return &Master{
		config:      config,
		metadata:    NewMetadataManager(),
		leaseManager: NewLeaseManager(time.Duration(config.LeaseTime) * time.Second),
		chunkServers: make(map[string]*ChunkServerInfo),
		shutdown:    make(chan struct{}),
	}
}

// Start initializes and starts the master server
func (m *Master) Start() error {
	// Load metadata from disk (recovery)
	if err := m.metadata.LoadFromDisk(); err != nil {
		log.Printf("Failed to load metadata: %v", err)
		// Continue without previous state
	}
	
	// Start RPC server
	listener, err := net.Listen("tcp", m.config.Address)
	if err != nil {
		return err
	}
	
	m.rpcServer = rpc.NewServer()
	// Register RPC methods
	m.registerRPCMethods()
	
	go m.rpcServer.Serve(listener)
	
	// Start background tasks
	go m.periodicCheckpoint()
	go m.monitorChunkServers()
	
	m.isRunning = true
	
	log.Printf("Master started on %s", m.config.Address)
	return nil
}

// Shutdown stops the master server
func (m *Master) Shutdown() error {
	if !m.isRunning {
		return nil
	}
	
	close(m.shutdown)
	
	// Perform checkpoint one last time
	if err := m.metadata.SaveToDisk(); err != nil {
		log.Printf("Failed to save metadata during shutdown: %v", err)
	}
	
	m.isRunning = false
	return m.rpcServer.Stop()
}

// RegisterChunkServer registers a new chunk server
func (m *Master) RegisterChunkServer(address string, chunks []common.ChunkHandle) {
	m.chunkServerMutex.Lock()
	defer m.chunkServerMutex.Unlock()
	
	m.chunkServers[address] = &ChunkServerInfo{
		Address:       address,
		LastHeartbeat: time.Now(),
		Chunks:        chunks,
		Available:     true,
	}
	
	// Update metadata with chunk locations
	for _, chunk := range chunks {
		m.metadata.AddChunkLocation(chunk, address)
	}
}

// HandleHeartbeat processes a heartbeat from a chunk server
func (m *Master) HandleHeartbeat(address string, chunks []common.ChunkHandle, capacity, used int64) {
	m.chunkServerMutex.Lock()
	defer m.chunkServerMutex.Unlock()
	
	if server, exists := m.chunkServers[address]; exists {
		server.LastHeartbeat = time.Now()
		server.Chunks = chunks
		server.Capacity = capacity
		server.Used = used
		server.Available = true
	} else {
		// New server
		m.chunkServers[address] = &ChunkServerInfo{
			Address:       address,
			LastHeartbeat: time.Now(),
			Chunks:        chunks,
			Available:     true,
			Capacity:      capacity,
			Used:          used,
		}
	}
}

// CreateFile creates a new file
func (m *Master) CreateFile(path string) (common.FileID, error) {
	return m.metadata.CreateFile(path)
}

// DeleteFile deletes a file
func (m *Master) DeleteFile(path string) error {
	return m.metadata.DeleteFile(path)
}

// GetFileInfo returns information about a file
func (m *Master) GetFileInfo(path string) (*common.FileMetadata, error) {
	return m.metadata.GetFileInfo(path)
}

// AllocateChunk allocates a new chunk for a file
func (m *Master) AllocateChunk(fileID common.FileID, index common.ChunkIndex) (common.ChunkHandle, []string, error) {
	// Pick N chunk servers for replication
	chunkServers, err := m.selectChunkServers(m.config.ChunkReplicaNum)
	if err != nil {
		return 0, nil, err
	}
	
	// Create a new chunk in metadata
	handle, err := m.metadata.CreateChunk(fileID, index, chunkServers)
	if err != nil {
		return 0, nil, err
	}
	
	// Ask chosen chunk servers to create the chunk
	for _, server := range chunkServers {
		// In a real implementation, this would be an async RPC call
		// Here we just assume it succeeds for simplicity
	}
	
	return handle, chunkServers, nil
}

// GetChunkLocations returns the locations of a chunk
func (m *Master) GetChunkLocations(handle common.ChunkHandle) ([]string, error) {
	return m.metadata.GetChunkLocations(handle)
}

// GrantLease grants a lease to a chunk server to be the primary
func (m *Master) GrantLease(handle common.ChunkHandle) (string, time.Time, error) {
	// Get chunk info
	chunk, err := m.metadata.GetChunkInfo(handle)
	if err != nil {
		return "", time.Time{}, err
	}
	
	// Check if there's already a valid lease
	if !chunk.LeaseExpiration.IsZero() && chunk.LeaseExpiration.After(time.Now()) {
		return chunk.PrimaryReplica, chunk.LeaseExpiration, nil
	}
	
	// Select a new primary from available replicas
	locations, err := m.metadata.GetChunkLocations(handle)
	if err != nil {
		return "", time.Time{}, err
	}
	
	if len(locations) == 0 {
		return "", time.Time{}, common.ErrNoAvailableServer
	}
	
	// Pick the first available server as primary (in a real implementation, 
	// we would use a more sophisticated selection process)
	primary := locations[0]
	expiration := time.Now().Add(time.Duration(m.config.LeaseTime) * time.Second)
	
	// Update lease in metadata
	if err := m.metadata.UpdateLease(handle, primary, expiration); err != nil {
		return "", time.Time{}, err
	}
	
	return primary, expiration, nil
}

// Report chunk corruption
func (m *Master) ReportChunkCorruption(handle common.ChunkHandle, server string) {
	m.metadata.RemoveChunkLocation(handle, server)
	
	// Check if we need to re-replicate
	locations, err := m.metadata.GetChunkLocations(handle)
	if err != nil {
		log.Printf("Error getting chunk locations: %v", err)
		return
	}
	
	if len(locations) < m.config.ChunkReplicaNum {
		go m.replicateChunk(handle)
	}
}

// Select N chunk servers for a new chunk
func (m *Master) selectChunkServers(n int) ([]string, error) {
	m.chunkServerMutex.RLock()
	defer m.chunkServerMutex.RUnlock()
	
	var available []string
	for addr, info := range m.chunkServers {
		if info.Available {
			available = append(available, addr)
		}
	}
	
	if len(available) < n {
		return nil, common.ErrNoAvailableServer
	}
	
	// In a real implementation, we would use a more sophisticated selection
	// based on load, storage capacity, network topology, etc.
	// For simplicity, we just pick the first N servers
	return available[:n], nil
}

// Replicate a chunk to maintain the desired replication level
func (m *Master) replicateChunk(handle common.ChunkHandle) {
	// Get current locations
	locations, err := m.metadata.GetChunkLocations(handle)
	if err != nil {
		log.Printf("Error getting chunk locations: %v", err)
		return
	}
	
	needed := m.config.ChunkReplicaNum - len(locations)
	if needed <= 0 {
		return
	}
	
	// Find available servers that don't already have this chunk
	m.chunkServerMutex.RLock()
	var candidates []string
	for addr, info := range m.chunkServers {
		if info.Available {
			found := false
			for _, loc := range locations {
				if loc == addr {
					found = true
					break
				}
			}
			if !found {
				candidates = append(candidates, addr)
			}
		}
	}
	m.chunkServerMutex.RUnlock()
	
	if len(candidates) < needed {
		log.Printf("Not enough servers to replicate chunk %v", handle)
		return
	}
	
	// Choose source server (again, this is simplified)
	source := locations[0]
	
	// Ask source to replicate to target servers
	for i := 0; i < needed; i++ {
		target := candidates[i]
		// This would be an RPC call in a real implementation
		// For now, just update metadata
		m.metadata.AddChunkLocation(handle, target)
	}
}

// Periodic tasks
func (m *Master) periodicCheckpoint() {
	ticker := time.NewTicker(time.Duration(m.config.CheckpointInterval) * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			if err := m.metadata.SaveToDisk(); err != nil {
				log.Printf("Failed to save metadata checkpoint: %v", err)
			}
		case <-m.shutdown:
			return
		}
	}
}

func (m *Master) monitorChunkServers() {
	ticker := time.NewTicker(time.Duration(m.config.HeartbeatInterval) * time.Second * 2)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			m.detectDeadChunkServers()
		case <-m.shutdown:
			return
		}
	}
}

func (m *Master) detectDeadChunkServers() {
	deadline := time.Now().Add(-time.Duration(m.config.HeartbeatInterval) * time.Second * 3)
	
	m.chunkServerMutex.Lock()
	defer m.chunkServerMutex.Unlock()
	
	for addr, info := range m.chunkServers {
		if info.LastHeartbeat.Before(deadline) {
			log.Printf("Chunk server %s appears to be dead", addr)
			info.Available = false
			
			// Re-replicate its chunks
			for _, chunk := range info.Chunks {
				go m.replicateChunk(chunk)
			}
		}
	}
}

func (m *Master) registerRPCMethods() {
	// Register RPC methods here
	// This is just a sketch, in a real implementation we would register
	// actual RPC methods here
}

// --------------------- pkg/master/metadata.go ---------------------

package master

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gfs/pkg/common"
)

// MetadataManager manages GFS metadata
type MetadataManager struct {
	// File namespace (path -> FileID)
	namespace     map[string]common.FileID
	namespaceLock sync.RWMutex
	
	// File metadata (FileID -> FileMetadata)
	files     map[common.FileID]*common.FileMetadata
	filesLock sync.RWMutex
	
	// Chunk metadata (ChunkHandle -> ChunkMetadata)
	chunks     map[common.ChunkHandle]*common.ChunkMetadata
	chunksLock sync.RWMutex
	
	// Next IDs
	nextChunkHandle common.ChunkHandle
	nextFileID      int
	idLock          sync.Mutex
	
	// Checkpoint location
	checkpointDir string
}

// NewMetadataManager creates a new metadata manager
func NewMetadataManager() *MetadataManager {
	return &MetadataManager{
		namespace:      make(map[string]common.FileID),
		files:          make(map[common.FileID]*common.FileMetadata),
		chunks:         make(map[common.ChunkHandle]*common.ChunkMetadata),
		nextChunkHandle: 1,
		nextFileID:      1,
		checkpointDir:   "./metadata",
	}
}

// LoadFromDisk loads metadata from disk during recovery
func (mm *MetadataManager) LoadFromDisk() error {
	// Create checkpoint directory if it doesn't exist
	if err := os.MkdirAll(mm.checkpointDir, 0755); err != nil {
		return err
	}
	
	// Load namespace
	namespaceFile := filepath.Join(mm.checkpointDir, "namespace.json")
	if data, err := os.ReadFile(namespaceFile); err == nil {
		if err := json.Unmarshal(data, &mm.namespace); err != nil {
			return err
		}
	}
	
	// Load files
	filesFile := filepath.Join(mm.checkpointDir, "files.json")
	if data, err := os.ReadFile(filesFile); err == nil {
		if err := json.Unmarshal(data, &mm.files); err != nil {
			return err
		}
	}
	
	// Load chunks
	chunksFile := filepath.Join(mm.checkpointDir, "chunks.json")
	if data, err := os.ReadFile(chunksFile); err == nil {
		if err := json.Unmarshal(data, &mm.chunks); err != nil {
			return err
		}
	}
	
	// Load next IDs
	idsFile := filepath.Join(mm.checkpointDir, "ids.json")
	if data, err := os.ReadFile(idsFile); err == nil {
		var ids struct {
			NextChunkHandle common.ChunkHandle `json:"next_chunk_handle"`
			NextFileID      int                `json:"next_file_id"`
		}
		if err := json.Unmarshal(data, &ids); err != nil {
			return err
		}
		mm.nextChunkHandle = ids.NextChunkHandle
		mm.nextFileID = ids.NextFileID
	}
	
	return nil
}

// SaveToDisk saves metadata to disk for checkpointing
func (mm *MetadataManager) SaveToDisk() error {
	// Create checkpoint directory if it doesn't exist
	if err := os.MkdirAll(mm.checkpointDir, 0755); err != nil {
		return err
	}
	
	// Save namespace
	mm.namespaceLock.RLock()
	namespaceData, err := json.Marshal(mm.namespace)
	mm.namespaceLock.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "namespace.json"), namespaceData, 0644); err != nil {
		return err
	}
	
	// Save files
	mm.filesLock.RLock()
	filesData, err := json.Marshal(mm.files)
	mm.filesLock.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "files.json"), filesData, 0644); err != nil {
		return err
	}
	
	// Save chunks
	mm.chunksLock.RLock()
	chunksData, err := json.Marshal(mm.chunks)
	mm.chunksLock.RUnlock()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(mm.checkpointDir, "chunks.json"), chunksData, 0644); err != nil {
		return err
	}
	
	// Save next IDs
	mm.idLock.Lock()
	ids := struct {
		NextChunkHandle common.ChunkHandle `json:"next_chunk_handle"`
		NextFileID      int                `json:"next_file_id"`
	}{
		NextChunkHandle: mm.nextChunkHandle,
		NextFileID:      mm.nextFileID,
	}
	mm.idLock.Unlock()
	
	idsData, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(mm.checkpointDir, "ids.json"), idsData, 0644)
}

// CreateFile creates a new file in the namespace
func (mm *MetadataManager) CreateFile(path string) (common.FileID, error) {
	// Check if file already exists
	mm.namespaceLock.RLock()
	_, exists := mm.namespace[path]
	mm.namespaceLock.RUnlock()
	
	if exists {
		return "", common.ErrInvalidArgument
	}
	
	// Generate new file ID
	mm.idLock.Lock()
	fileID := common.FileID(filepath.Base(path) + "-" + time.Now().Format("20060102-150405") + "-" + 
		      		      		   	   	 	  		 	   	  	 	 		  	 			           	 	 	 	   																									 	     			   			 	         string(mm.nextFileID))
	mm.nextFileID++
	mm.idLock.Unlock()
	
	now := time.Now()
	fileMetadata := &common.FileMetadata{
		ID:             fileID,
		Path:           path,
		Size:           0,
		ChunkCount:     0,
		ChunkHandles:   []common.ChunkHandle{},
		CreationTime:   now,
		ModificationTime: now,
	}
	
	// Update maps
	mm.filesLock.Lock()
	mm.files[fileID] = fileMetadata
	mm.filesLock.Unlock()
	
	mm.namespaceLock.Lock()
	mm.namespace[path] = fileID
	mm.namespaceLock.Unlock()
	
	return fileID, nil
}

// DeleteFile deletes a file from the namespace
func (mm *MetadataManager) DeleteFile(path string) error {
	// Get the file ID
	mm.namespaceLock.RLock()
	fileID, exists := mm.namespace[path]
	mm.namespaceLock.RUnlock()
	
	if !exists {
		return common.ErrFileNotFound
	}
	
	// Get chunks to delete
	mm.filesLock.RLock()
	file, exists := mm.files[fileID]
	if !exists {
		mm.filesLock.RUnlock()
		return common.ErrFileNotFound
	}
	
	chunkHandles := make([]common.ChunkHandle, len(file.ChunkHandles))
	copy(chunkHandles, file.ChunkHandles)
	mm.filesLock.RUnlock()
	
	// Delete chunks
	mm.chunksLock.Lock()
	for _, handle := range chunkHandles {
		delete(mm.chunks, handle)
	}
	mm.chunksLock.Unlock()
	
	// Delete file metadata
	mm.filesLock.Lock()
	delete(mm.files, fileID)
	mm.filesLock.Unlock()
	
	// Remove from namespace
	mm.namespaceLock.Lock()
	delete(mm.namespace, path)
	mm.namespaceLock.Unlock()
	
	return nil
}

// GetFileInfo returns metadata for a file
func (mm *MetadataManager) GetFileInfo(path string) (*common.FileMetadata, error) {
	// Get the file ID
	mm.namespaceLock.RLock()
	fileID, exists := mm.namespace[path]
	mm.namespaceLock.RUnlock()
	
	if !exists {
		return nil, common.ErrFileNotFound
	}
	
	// Get file metadata
	mm.filesLock.RLock()
	defer mm.filesLock.RUnlock()
	
	file, exists := mm.files[fileID]
	if !exists {
		return nil, common.ErrFileNotFound
	}
	
	// Create a copy to avoid concurrent access issues
	fileCopy := *file
	return &fileCopy, nil
}

// GetChunkHandle returns the chunk handle for a specific file index
func (mm *MetadataManager) GetChunkHandle(fileID common.FileID, index common.ChunkIndex) (common.ChunkHandle, error) {
	mm.filesLock.RLock()
	file, exists := mm.files[fileID]
	mm.filesLock.RUnlock()
	
	if !exists {
		return 0, common.ErrFileNotFound
	}
	
	if int(index) >= len(file.ChunkHandles) {
		return 0, common.ErrChunkNotFound
	}
	
	return file.ChunkHandles[index], nil
}

// CreateChunk creates a new chunk for a file
func (mm *MetadataManager) CreateChunk(fileID common.FileID, index common.ChunkIndex, locations []string) (common.ChunkHandle, error) {
	// Check if file exists
	mm.filesLock.RLock()
	file, exists := mm.files[fileID]
	mm.filesLock.RUnlock()
	
	if !exists {
		return 0, common.ErrFileNotFound
	}
	
	// Generate a new chunk handle
	mm.idLock.Lock()
	handle := mm.nextChunkHandle
	mm.nextChunkHandle++
	mm.idLock.Unlock()
	
	// Create chunk metadata
	chunk := &common.ChunkMetadata{
		Handle:    handle,
		FileID:    fileID,
		Index:     index,
		Version:   1, // Initial version
		Size:      0,
		Locations: locations,
	}
	
	// Update chunk map
	mm.chunksLock.Lock()
	mm.chunks[handle] = chunk
	mm.chunksLock.Unlock()
	
	// Update file metadata
	mm.filesLock.Lock()
	defer mm.filesLock.Unlock()
	
	// Make sure file still exists
	file, exists = mm.files[fileID]
	if !exists {
		return 0, common.ErrFileNotFound
	}
	
	// Ensure the slice is big enough
	for int(index) >= len(file.ChunkHandles) {
		file.ChunkHandles = append(file.ChunkHandles, 0) // 0 means no chunk allocated
	}
	
	file.ChunkHandles[index] = handle
	file.ChunkCount = len(file.ChunkHandles)
	file.ModificationTime = time.Now()
	
	return handle, nil
}

// GetChunkInfo returns metadata for a chunk
func (mm *MetadataManager) GetChunkInfo(handle common.ChunkHandle) (*common.ChunkMetadata, error) {
	mm.chunksLock.RLock()
	defer mm.chunksLock.RUnlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return nil, common.ErrChunkNotFound
	}
	
	// Create a copy to avoid concurrent access issues
	chunkCopy := *chunk
	return &chunkCopy, nil
}

// GetChunkLocations returns the locations of a chunk
func (mm *MetadataManager) GetChunkLocations(handle common.ChunkHandle) ([]string, error) {
	mm.chunksLock.RLock()
	defer mm.chunksLock.RUnlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return nil, common.ErrChunkNotFound
	}
	
	// Create a copy of the locations slice
	locations := make([]string, len(chunk.Locations))
	copy(locations, chunk.Locations)
	
	return locations, nil
}

// AddChunkLocation adds a new location for a chunk
func (mm *MetadataManager) AddChunkLocation(handle common.ChunkHandle, location string) {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return
	}
	
	// Check if location already exists
	for _, loc := range chunk.Locations {
		if loc == location {
			return
		}
	}
	
	// Add the new location
	chunk.Locations = append(chunk.Locations, location)
}

// RemoveChunkLocation removes a location for a chunk
func (mm *MetadataManager) RemoveChunkLocation(handle common.ChunkHandle, location string) {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return
	}
	
	// Remove the location
	for i, loc := range chunk.Locations {
		if loc == location {
			// Remove by swapping with the last element and truncating
			lastIndex := len(chunk.Locations) - 1
			chunk.Locations[i] = chunk.Locations[lastIndex]
			chunk.Locations = chunk.Locations[:lastIndex]
			return
		}
	}
}

// UpdateLease updates the lease information for a chunk
func (mm *MetadataManager) UpdateLease(handle common.ChunkHandle, primary string, expiration time.Time) error {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return common.ErrChunkNotFound
	}
	
	chunk.PrimaryReplica = primary
	chunk.LeaseExpiration = expiration
	return nil
}

// UpdateChunkVersion increments the version of a chunk
func (mm *MetadataManager) UpdateChunkVersion(handle common.ChunkHandle) (common.ChunkVersion, error) {
	mm.chunksLock.Lock()
	defer mm.chunksLock.Unlock()
	
	chunk, exists := mm.chunks[handle]
	if !exists {
		return 0, common.ErrChunkNotFound
	}
	
	chunk.Version++
	return chunk.Version, nil
}

// UpdateFileSize updates the size of a file
func (mm *MetadataManager) UpdateFileSize(fileID common.FileID, newSize int64) error {
	mm.filesLock.Lock()
	defer mm.filesLock.Unlock()
	
	file, exists := mm.files[fileID]
	if !exists {
		return common.ErrFileNotFound
	}
	
	file.Size = newSize
	file.ModificationTime = time.Now()
	return nil
}

// --------------------- pkg/master/lease.go ---------------------

package master

import (
	"sync"
	"time"

	"gfs/pkg/common"
)

// LeaseManager manages chunk leases
type LeaseManager struct {
	// Chunk handle -> lease expiration time
	leases map[common.ChunkHandle]time.Time
	mutex  sync.RWMutex
	
	// Default lease duration
	leaseDuration time.Duration
}

// NewLeaseManager creates a new lease manager
func NewLeaseManager(duration time.Duration) *LeaseManager {
	return &LeaseManager{
		leases:        make(map[common.ChunkHandle]time.Time),
		leaseDuration: duration,
	}
}

// GrantLease grants a lease for a chunk
func (lm *LeaseManager) GrantLease(handle common.ChunkHandle) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	
	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[handle] = expiration
	
	return expiration, nil
}

// ExtendLease extends an existing lease
func (lm *LeaseManager) ExtendLease(handle common.ChunkHandle) (time.Time, error) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	
	_, exists := lm.leases[handle]
	if !exists {
		return time.Time{}, common.ErrChunkNotFound
	}
	
	expiration := time.Now().Add(lm.leaseDuration)
	lm.leases[handle] = expiration
	
	return expiration, nil
}

// CheckLease checks if a lease is valid
func (lm *LeaseManager) CheckLease(handle common.ChunkHandle) (bool, time.Time) {
	lm.mutex.RLock()
	defer lm.mutex.RUnlock()
	
	expiration, exists := lm.leases[handle]
	if !exists {
		return false, time.Time{}
	}
	
	return time.Now().Before(expiration), expiration
}

// RevokeLease revokes a lease
func (lm *LeaseManager) RevokeLease(handle common.ChunkHandle) {
	lm.mutex.Lock()
	defer lm.mutex.Unlock()
	
	delete(lm.leases, handle)
}

// --------------------- pkg/chunkserver/server.go ---------------------

package chunkserver

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"log"
	"net"
	"sync"
	"time"

	"gfs/internal/rpc"
	"gfs/pkg/common"
)

// ChunkServer serves chunks of files
type ChunkServer struct {
	config common.ChunkServerConfig
	
	// Storage management
	storageManager *StorageManager
	
	// Chunks managed by this server
	chunks     map[common.ChunkHandle]*Chunk
	chunkMutex sync.RWMutex
	
	// Mutation queue
	mutations     map[common.ChunkHandle][]common.Mutation
	mutationMutex sync.RWMutex
	
	// Server state
	isRunning bool
	shutdown  chan struct{}
	
	// RPC server
	rpcServer *rpc.Server
}

// NewChunkServer creates a new chunk server
func NewChunkServer(config common.ChunkServerConfig) (*ChunkServer, error) {
	// Create storage manager
	storageManager, err := NewStorageManager(config.StorageRoot)
	if err != nil {
		return nil, err
	}
	
	return &ChunkServer{
		config:         config,
		storageManager: storageManager,
		chunks:         make(map[common.ChunkHandle]*Chunk),
		mutations:      make(map[common.ChunkHandle][]common.Mutation),
		shutdown:       make(chan struct{}),
	}, nil
}

// Start initializes and starts the chunk server
func (cs *ChunkServer) Start() error {
	// Load existing chunks from disk
	handles, err := cs.storageManager.LoadChunks()
	if err != nil {
		return err
	}
	
	// Register chunks in memory
	for _, handle := range handles {
		chunk, err := cs.loadChunk(handle)
		if err != nil {
			log.Printf("Failed to load chunk %v: %v", handle, err)
			continue
		}
		
		cs.chunkMutex.Lock()
		cs.chunks[handle] = chunk
		cs.chunkMutex.Unlock()
	}
	
	// Start RPC server
	listener, err := net.Listen("tcp", cs.config.Address)
	if err != nil {
		return err
	}
	
	cs.rpcServer = rpc.NewServer()
	// Register RPC methods
	cs.registerRPCMethods()
	
	go cs.rpcServer.Serve(listener)
	
	// Start background tasks
	go cs.sendHeartbeats()
	go cs.applyMutations()
	
	cs.isRunning = true
	
	// Register with master
	if err := cs.registerWithMaster(); err != nil {
		log.Printf("Failed to register with master: %v", err)
		// Continue anyway
	}
	
	log.Printf("Chunk server started on %s", cs.config.Address)
	return nil
}

// Shutdown stops the chunk server
func (cs *ChunkServer) Shutdown() error {
	if !cs.isRunning {
		return nil
	}
	
	close(cs.shutdown)
	
	// Wait for pending operations to complete
	// In a real implementation, we would have a wait group
	time.Sleep(1 * time.Second)
	
	cs.isRunning = false
	return cs.rpcServer.Stop()
}

// CreateChunk creates a new chunk
func (cs *ChunkServer) CreateChunk(handle common.ChunkHandle, version common.ChunkVersion) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()
	
	// Check if chunk already exists
	if _, exists := cs.chunks[handle]; exists {
		return nil // Already exists, nothing to do
	}
	
	// Create a new chunk
	chunk := NewChunk(handle, version)
	
	// Write to disk
	if err := cs.storageManager.CreateChunk(handle); err != nil {
		return err
	}
	
	// Update metadata
	if err := cs.storageManager.UpdateMetadata(handle, version, 0); err != nil {
		// Try to clean up
		cs.storageManager.DeleteChunk(handle)
		return err
	}
	
	cs.chunks[handle] = chunk
	return nil
}

// ReadChunk reads data from a chunk
func (cs *ChunkServer) ReadChunk(handle common.ChunkHandle, offset, length int64) ([]byte, error) {
	cs.chunkMutex.RLock()
	chunk, exists := cs.chunks[handle]
	cs.chunkMutex.RUnlock()
	
	if !exists {
		return nil, common.ErrChunkNotFound
	}
	
	// Validate offset and length
	if offset < 0 || offset >= common.ChunkSize {
		return nil, common.ErrInvalidOffset
	}
	
	// Cap length to end of chunk
	if offset+length > common.ChunkSize {
		length = common.ChunkSize - offset
	}
	
	// Read from storage
	data, err := cs.storageManager.ReadChunk(handle, offset, length)
	if err != nil {
		return nil, err
	}
	
	// Verify checksum
	// In a real implementation, we would verify checksums for each block
	
	return data, nil
}

// WriteChunk writes data to a chunk
func (cs *ChunkServer) WriteChunk(handle common.ChunkHandle, offset int64, data []byte) error {
	cs.chunkMutex.RLock()
	chunk, exists := cs.chunks[handle]
	cs.chunkMutex.RUnlock()
	
	if !exists {
		return common.ErrChunkNotFound
	}
	
	// Validate offset
	if offset < 0 || offset >= common.ChunkSize {
		return common.ErrInvalidOffset
	}
	
	// Check if there's enough space
	if offset+int64(len(data)) > common.ChunkSize {
		return common.ErrInvalidArgument
	}
	
	// Write to storage
	if err := cs.storageManager.WriteChunk(handle, offset, data); err != nil {
		return err
	}
	
	// Update chunk size if necessary
	newSize := offset + int64(len(data))
	if newSize > chunk.Size {
		cs.chunkMutex.Lock()
		chunk.Size = newSize
		cs.chunkMutex.Unlock()
		
		// Update metadata
		if err := cs.storageManager.UpdateMetadata(handle, chunk.Version, newSize); err != nil {
			// Log but don't fail
			log.Printf("Failed to update metadata for chunk %v: %v", handle, err)
		}
	}
	
	return nil
}

// ApplyMutation applies a mutation to a chunk
func (cs *ChunkServer) ApplyMutation(handle common.ChunkHandle, mutation common.Mutation) error {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()
	
	// Add to mutation queue
	cs.mutations[handle] = append(cs.mutations[handle], mutation)
	
	return nil
}

// AppendChunk performs an atomic append to a chunk
func (cs *ChunkServer) AppendChunk(handle common.ChunkHandle, data []byte) (int64, error) {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()
	
	chunk, exists := cs.chunks[handle]
	if !exists {
		return 0, common.ErrChunkNotFound
	}
	
	// Check if there's enough space
	if chunk.Size+int64(len(data)) > common.ChunkSize {
		return 0, common.ErrInvalidOffset
	}
	
	// Append to storage
	offset := chunk.Size
	if err := cs.storageManager.WriteChunk(handle, offset, data); err != nil {
		return 0, err
	}
	
	// Update chunk size
	chunk.Size += int64(len(data))
	
	// Update metadata
	if err := cs.storageManager.UpdateMetadata(handle, chunk.Version, chunk.Size); err != nil {
		// Log but don't fail
		log.Printf("Failed to update metadata for chunk %v: %v", handle, err)
	}
	
	return offset, nil
}

// UpdateVersion updates the version of a chunk
func (cs *ChunkServer) UpdateVersion(handle common.ChunkHandle, version common.ChunkVersion) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()
	
	chunk, exists := cs.chunks[handle]
	if !exists {
		return common.ErrChunkNotFound
	}
	
	chunk.Version = version
	
	// Update metadata
	return cs.storageManager.UpdateMetadata(handle, version, chunk.Size)
}

// DeleteChunk deletes a chunk
func (cs *ChunkServer) DeleteChunk(handle common.ChunkHandle) error {
	cs.chunkMutex.Lock()
	delete(cs.chunks, handle)
	cs.chunkMutex.Unlock()
	
	cs.mutationMutex.Lock()
	delete(cs.mutations, handle)
	cs.mutationMutex.Unlock()
	
	// Delete from storage
	return cs.storageManager.DeleteChunk(handle)
}

// Background tasks
func (cs *ChunkServer) sendHeartbeats() {
	ticker := time.NewTicker(time.Duration(cs.config.HeartbeatInterval) * time.Second)
	defer ticker.Stop()
	
	// Create RPC client for the master
	masterClient, err := rpc.NewClient(cs.config.MasterAddress)
	if err != nil {
		log.Printf("Failed to create RPC client for master: %v", err)
		// Continue without a client, we'll try to create it again on the next heartbeat
	}
	defer func() {
		if masterClient != nil {
			masterClient.Close()
		}
	}()
	
	for {
		select {
		case <-ticker.C:
			// If we don't have a valid client, try to create one
			if masterClient == nil {
				masterClient, err = rpc.NewClient(cs.config.MasterAddress)
				if err != nil {
					log.Printf("Failed to create RPC client for master: %v", err)
					continue
				}
			}
			
			// Get list of chunks
			cs.chunkMutex.RLock()
			handles := make([]common.ChunkHandle, 0, len(cs.chunks))
			for handle := range cs.chunks {
				handles = append(handles, handle)
			}
			cs.chunkMutex.RUnlock()
			
			// Get storage stats
			capacity, used := cs.storageManager.GetStats()
			
			// Prepare heartbeat RPC request
			args := &rpc.HeartbeatArgs{
				Address:  cs.config.Address,
				Chunks:   handles,
				Capacity: capacity,
				Used:     used,
			}
			
			// Prepare reply structure
			reply := &rpc.HeartbeatReply{}
			
			// Make the RPC call with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			
			// Create a channel to receive the result
			done := make(chan error, 1)
			go func() {
				// Call the Heartbeat RPC method
				err := masterClient.Client.Call("MasterService.Heartbeat", args, reply)
				done <- err
			}()
			
			// Wait for either the RPC to complete or the timeout
			select {
			case err := <-done:
				if err != nil {
					log.Printf("Heartbeat RPC failed: %v", err)
					// Close the client so we'll create a new one on the next heartbeat
					masterClient.Close()
					masterClient = nil
				} else if reply.Status != common.StatusOK {
					log.Printf("Heartbeat rejected by master with status: %v", reply.Status)
				} else {
					log.Printf("Heartbeat succeeded: reported %d chunks, %d/%d storage used", 
						len(handles), used, capacity)
					
					// Process any instructions from the master in the reply
					// For example, the master might ask us to delete certain chunks
					// or initiate chunk replication
					if len(reply.ChunksToDelete) > 0 {
						log.Printf("Master requested deletion of %d chunks", len(reply.ChunksToDelete))
						go cs.deleteChunks(reply.ChunksToDelete)
					}
					
					if len(reply.ChunksToReplicate) > 0 {
						log.Printf("Master requested replication of %d chunks", len(reply.ChunksToReplicate))
						go cs.replicateChunks(reply.ChunksToReplicate)
					}
				}
			case <-ctx.Done():
				log.Printf("Heartbeat timed out after 5 seconds")
				// Close the client so we'll create a new one on the next heartbeat
				masterClient.Close()
				masterClient = nil
			}
			
		case <-cs.shutdown:
			return
		}
	}
}

func (cs *ChunkServer) applyMutations() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			cs.processPendingMutations()
		case <-cs.shutdown:
			return
		}
	}
}

func (cs *ChunkServer) processPendingMutations() {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()
	
	for handle, mutations := range cs.mutations {
		if len(mutations) == 0 {
			continue
		}
		
		// Process mutations in order
		for _, mutation := range mutations {
			var err error
			
			switch mutation.Type {
			case common.MutationWrite:
				err = cs.WriteChunk(handle, mutation.Offset, mutation.Data)
			case common.MutationAppend:
				_, err = cs.AppendChunk(handle, mutation.Data)
			case common.MutationDelete:
				err = cs.DeleteChunk(handle)
			}
			
			if err != nil {
				log.Printf("Failed to apply mutation to chunk %v: %v", handle, err)
			}
		}
		
		// Clear processed mutations
		delete(cs.mutations, handle)
	}
}

func (cs *ChunkServer) loadChunk(handle common.ChunkHandle) (*Chunk, error) {
	// Read metadata
	version, size, err := cs.storageManager.ReadMetadata(handle)
	if err != nil {
		return nil, err
	}
	
	// Create chunk object
	chunk := NewChunk(handle, version)
	chunk.Size = size
	
	return chunk, nil
}

func (cs *ChunkServer) registerWithMaster() error {
	// In a real implementation, this would be an RPC call
	// Get list of chunks
	cs.chunkMutex.RLock()
	handles := make([]common.ChunkHandle, 0, len(cs.chunks))
	for handle := range cs.chunks {
		handles = append(handles, handle)
	}
	cs.chunkMutex.RUnlock()
	
	log.Printf("Registering with master at %s with %d chunks", 
		cs.config.MasterAddress, len(handles))
	
	return nil
}

func (cs *ChunkServer) registerRPCMethods() {
	// Register RPC methods here
	// This is just a sketch, in a real implementation we would register
	// actual RPC methods
}

// --------------------- pkg/chunkserver/chunk.go ---------------------

package chunkserver

import (
	"sync"

	"gfs/pkg/common"
)

// Chunk represents a chunk in memory
type Chunk struct {
	Handle  common.ChunkHandle
	Version common.ChunkVersion
	Size    int64
	mutex   sync.RWMutex
}

// NewChunk creates a new chunk
func NewChunk(handle common.ChunkHandle, version common.ChunkVersion) *Chunk {
	return &Chunk{
		Handle:  handle,
		Version: version,
		Size:    0,
	}
}

// --------------------- pkg/chunkserver/storage.go ---------------------

package chunkserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"gfs/pkg/common"
)

// StorageManager manages local storage for chunks
type StorageManager struct {
	// Root directory for chunk storage
	root string
	
	// Mutex for storage operations
	mutex sync.RWMutex
}

// ChunkMetadata file format:
// 0-7: Version (uint64)
// 8-15: Size (int64)
// 16-19: CRC32 checksum (uint32)

// NewStorageManager creates a new storage manager
func NewStorageManager(root string) (*StorageManager, error) {
	// Create root directory if it doesn't exist
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	
	return &StorageManager{
		root: root,
	}, nil
}

// LoadChunks loads existing chunks from disk
func (sm *StorageManager) LoadChunks() ([]common.ChunkHandle, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	
	var handles []common.ChunkHandle
	
	// Walk through chunk directory
	err := filepath.Walk(sm.root, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		if !info.IsDir() && filepath.Ext(path) == ".chunk" {
			// Extract handle from filename
			filename := filepath.Base(path)
			var handle common.ChunkHandle
			_, err := fmt.Sscanf(filename, "%d.chunk", &handle)
			if err != nil {
				return nil // Skip invalid files
			}
			
			handles = append(handles, handle)
		}
		
		return nil
	})
	
	return handles, err
}

// CreateChunk creates a new chunk file
func (sm *StorageManager) CreateChunk(handle common.ChunkHandle) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	// Create chunk data file
	chunkPath := sm.getChunkPath(handle)
	file, err := os.Create(chunkPath)
	if err != nil {
		return err
	}
	file.Close()
	
	// Create metadata file
	metaPath := sm.getMetadataPath(handle)
	metaFile, err := os.Create(metaPath)
	if err != nil {
		// Clean up chunk file
		os.Remove(chunkPath)
		return err
	}
	defer metaFile.Close()
	
	// Initialize metadata
	var buf [20]byte // Version (8) + Size (8) + CRC32 (4)
	binary.LittleEndian.PutUint64(buf[0:8], 1) // Initial version
	binary.LittleEndian.PutUint64(buf[8:16], 0) // Initial size
	binary.LittleEndian.PutUint32(buf[16:20], 0) // Initial checksum
	
	_, err = metaFile.Write(buf[:])
	return err
}

// ReadChunk reads data from a chunk
func (sm *StorageManager) ReadChunk(handle common.ChunkHandle, offset, length int64) ([]byte, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	
	// Open chunk file
	file, err := os.Open(sm.getChunkPath(handle))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	// Seek to offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	
	// Read data
	data := make([]byte, length)
	n, err := file.Read(data)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	
	return data[:n], nil
}

// WriteChunk writes data to a chunk
func (sm *StorageManager) WriteChunk(handle common.ChunkHandle, offset int64, data []byte) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	// Open chunk file
	file, err := os.OpenFile(sm.getChunkPath(handle), os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// Seek to offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	
	// Write data
	_, err = file.Write(data)
	return err
}

// DeleteChunk deletes a chunk
func (sm *StorageManager) DeleteChunk(handle common.ChunkHandle) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	// Remove chunk file
	if err := os.Remove(sm.getChunkPath(handle)); err != nil && !os.IsNotExist(err) {
		return err
	}
	
	// Remove metadata file
	if err := os.Remove(sm.getMetadataPath(handle)); err != nil && !os.IsNotExist(err) {
		return err
	}
	
	return nil
}

// ReadMetadata reads metadata for a chunk
func (sm *StorageManager) ReadMetadata(handle common.ChunkHandle) (common.ChunkVersion, int64, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()
	
	// Open metadata file
	file, err := os.Open(sm.getMetadataPath(handle))
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()
	
	// Read metadata
	var buf [20]byte
	_, err = io.ReadFull(file, buf[:])
	if err != nil {
		return 0, 0, err
	}
	
	version := common.ChunkVersion(binary.LittleEndian.Uint64(buf[0:8]))
	size := int64(binary.LittleEndian.Uint64(buf[8:16]))
	
	return version, size, nil
}

// UpdateMetadata updates metadata for a chunk
func (sm *StorageManager) UpdateMetadata(handle common.ChunkHandle, version common.ChunkVersion, size int64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	
	// Open metadata file
	file, err := os.OpenFile(sm.getMetadataPath(handle), os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	
	// Write metadata
	var buf [20]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(version))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(size))
	
	// In a real implementation, we would compute the checksum
	// For simplicity, we just use 0 here
	binary.LittleEndian.PutUint32(buf[16:20], 0)
	
	_, err = file.Write(buf[:])
	return err
}

// GetStats returns storage stats
func (sm *StorageManager) GetStats() (int64, int64) {
	var stat syscall.Statfs_t
	
	err := syscall.Statfs(sm.root, &stat)
	if err != nil {
		return 0, 0
	}
	
	capacity := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := capacity - available
	
	return int64(capacity), int64(used)
}

// Helper methods
func (sm *StorageManager) getChunkPath(handle common.ChunkHandle) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.chunk", handle))
}

func (sm *StorageManager) getMetadataPath(handle common.ChunkHandle) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.meta", handle))
}

// --------------------- pkg/client/client.go ---------------------

package client

import (
	"errors"
	"sync"
	"time"

	"gfs/pkg/common"
)

// GFSClient provides access to the GFS
type GFSClient struct {
	config common.ClientConfig
	
	// File to chunk handle cache
	fileCache     map[common.FileID][]common.ChunkHandle
	fileCacheLock sync.RWMutex
	fileCacheExp  map[common.FileID]time.Time
	
	// Chunk to locations cache
	chunkCache     map[common.ChunkHandle][]string
	chunkCacheLock sync.RWMutex
	chunkCacheExp  map[common.ChunkHandle]time.Time
}

// NewClient creates a new GFS client
func NewClient(config common.ClientConfig) *GFSClient {
	return &GFSClient{
		config:        config,
		fileCache:     make(map[common.FileID][]common.ChunkHandle),
		fileCacheExp:  make(map[common.FileID]time.Time),
		chunkCache:    make(map[common.ChunkHandle][]string),
		chunkCacheExp: make(map[common.ChunkHandle]time.Time),
	}
}

// Create creates a new file
func (c *GFSClient) Create(path string) error {
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return nil
	return nil
}

// Delete deletes a file
func (c *GFSClient) Delete(path string) error {
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return nil
	return nil
}

// Open gets file information for subsequent operations
func (c *GFSClient) Open(path string) (common.FileID, error) {
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return a dummy file ID
	return common.FileID("dummy"), nil
}

// Write writes data to a file
func (c *GFSClient) Write(fileID common.FileID, offset int64, data []byte) error {
	return c.write(fileID, offset, data, false)
}

// write implements the core write logic for both Write and Append
func (c *GFSClient) write(fileID common.FileID, offset int64, data []byte, isAppend bool) error {
	// Get file's chunk handles
	handles, err := c.getChunkHandles(fileID)
	if err != nil {
		return err
	}
	
	// Calculate which chunk the write starts in
	startChunk := offset / common.ChunkSize
	endChunk := (offset + int64(len(data)) - 1) / common.ChunkSize
	
	// If we need more chunks than exist, allocate them
	if int(endChunk) >= len(handles) {
		// In a real implementation, this would be an RPC call to allocate new chunks
		return errors.New("not enough chunks allocated")
	}
	
	// Process each affected chunk
	for chunk := startChunk; chunk <= endChunk; chunk++ {
		// Calculate data slice for this chunk
		chunkOffset := offset - (chunk * common.ChunkSize)
		if chunkOffset < 0 {
			chunkOffset = 0
		}
		
		var chunkData []byte
		remainingBytes := common.ChunkSize - chunkOffset
		if int64(len(data)) > remainingBytes {
			chunkData = data[:remainingBytes]
			data = data[remainingBytes:]
		} else {
			chunkData = data
			data = nil
		}
		
		// For append operations, we need to ask the primary for the actual offset
		if isAppend {
			// In a real implementation, this would be an RPC call to the primary
			// to get the actual append offset
			chunkOffset = 0 // Simplification
		}
		
		// Get primary location for this chunk
		primary, err := c.getPrimaryForChunk(handles[chunk])
		if err != nil {
			return err
		}
		
		// Get secondaries (other replicas)
		locations, err := c.getChunkLocations(handles[chunk])
		if err != nil {
			return err
		}
		
		var secondaries []string
		for _, loc := range locations {
			if loc != primary {
				secondaries = append(secondaries, loc)
			}
		}
		
		// Push data to all replicas
		if err := c.pushDataToReplicas(chunkData, primary, secondaries); err != nil {
			return err
		}
		
		// Send write request to primary
		// In a real implementation, this would be an RPC call to the primary
		// For simplicity, we just pretend it succeeded
		
		// Update offset for the next chunk
		offset += int64(len(chunkData))
	}
	
	return nil
}

// Helper methods for client operations
func (c *GFSClient) getChunkHandles(fileID common.FileID) ([]common.ChunkHandle, error) {
	c.fileCacheLock.RLock()
	handles, exists := c.fileCache[fileID]
	expiry, _ := c.fileCacheExp[fileID]
	c.fileCacheLock.RUnlock()
	
	// If we have a valid cache entry, use it
	if exists && time.Now().Before(expiry) {
		return handles, nil
	}
	
	// Otherwise, fetch from master
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return a dummy value
	handles = []common.ChunkHandle{1, 2, 3}
	
	// Update cache
	expiry = time.Now().Add(time.Duration(c.config.CacheTimeout) * time.Second)
	c.fileCacheLock.Lock()
	c.fileCache[fileID] = handles
	c.fileCacheExp[fileID] = expiry
	c.fileCacheLock.Unlock()
	
	return handles, nil
}

func (c *GFSClient) getChunkLocations(handle common.ChunkHandle) ([]string, error) {
	c.chunkCacheLock.RLock()
	locations, exists := c.chunkCache[handle]
	expiry, _ := c.chunkCacheExp[handle]
	c.chunkCacheLock.RUnlock()
	
	// If we have a valid cache entry, use it
	if exists && time.Now().Before(expiry) {
		return locations, nil
	}
	
	// Otherwise, fetch from master
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return dummy values
	locations = []string{"server1:8001", "server2:8001", "server3:8001"}
	
	// Update cache
	expiry = time.Now().Add(time.Duration(c.config.CacheTimeout) * time.Second)
	c.chunkCacheLock.Lock()
	c.chunkCache[handle] = locations
	c.chunkCacheExp[handle] = expiry
	c.chunkCacheLock.Unlock()
	
	return locations, nil
}

func (c *GFSClient) getPrimaryForChunk(handle common.ChunkHandle) (string, error) {
	// In a real implementation, this would be an RPC call to the master
	// For simplicity, we just return a dummy value
	locations, err := c.getChunkLocations(handle)
	if err != nil {
		return "", err
	}
	
	if len(locations) == 0 {
		return "", common.ErrNoAvailableServer
	}
	
	return locations[0], nil
}

func (c *GFSClient) pushDataToReplicas(data []byte, primary string, secondaries []string) error {
	// In a real implementation, this would involve RPC calls to all replicas
	// For simplicity, we just pretend it succeeded
	return nil
}

// Append appends data to a file
func (c *GFSClient) Append(fileID common.FileID, data []byte) (int64, error) {
	// Calculate file size to determine append offset
	// In a real implementation, we would get this from the master
	fileSize := int64(0)
	
	if err := c.write(fileID, fileSize, data, true); err != nil {
		return 0, err
	}
	
	return fileSize, nil
}

// --------------------- pkg/client/read.go ---------------------

package client

import (
	"io"
	"math"

	"gfs/pkg/common"
)

// Read reads data from a file
func (c *GFSClient) Read(fileID common.FileID, offset, length int64) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}
	
	// Get file's chunk handles
	handles, err := c.getChunkHandles(fileID)
	if err != nil {
		return nil, err
	}
	
	// Calculate which chunks we need to read from
	startChunk := offset / common.ChunkSize
	endChunk := (offset + length - 1) / common.ChunkSize
	
	if int(startChunk) >= len(handles) {
		return nil, common.ErrInvalidOffset
	}
	
	// Cap end chunk to available chunks
	if int(endChunk) >= len(handles) {
		endChunk = int64(len(handles) - 1)
		length = (endChunk+1)*common.ChunkSize - offset
	}
	
	// Allocate buffer for the result
	result := make([]byte, 0, length)
	
	// Read from each chunk
	bytesLeft := length
	for chunkIdx := startChunk; chunkIdx <= endChunk; chunkIdx++ {
		// Calculate offset within this chunk
		chunkOffset := int64(0)
		if chunkIdx == startChunk {
			chunkOffset = offset % common.ChunkSize
		}
		
		// Calculate how many bytes to read from this chunk
		bytesToRead := common.ChunkSize - chunkOffset
		if bytesToRead > bytesLeft {
			bytesToRead = bytesLeft
		}
		
		// Read from the chunk
		data, err := c.readChunk(handles[chunkIdx], chunkOffset, bytesToRead)
		if err != nil {
			return nil, err
		}
		
		// Append to result
		result = append(result, data...)
		
		// Update bytes left
		bytesLeft -= int64(len(data))
		if bytesLeft <= 0 {
			break
		}
	}
	
	return result, nil
}

// ReadChunkAt reads a single chunk at a specific position
func (c *GFSClient) ReadChunkAt(fileID common.FileID, chunkIndex int) ([]byte, error) {
	// Get file's chunk handles
	handles, err := c.getChunkHandles(fileID)
	if err != nil {
		return nil, err
	}
	
	if chunkIndex >= len(handles) {
		return nil, common.ErrInvalidOffset
	}
	
	// Read the entire chunk
	return c.readChunk(handles[chunkIndex], 0, common.ChunkSize)
}

// ReadAt reads from a specific offset
func (c *GFSClient) ReadAt(fileID common.FileID, p []byte, off int64) (int, error) {
	// If buffer is empty, we're done
	if len(p) == 0 {
		return 0, nil
	}
	
	// Read from file
	data, err := c.Read(fileID, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	
	// Copy into provided buffer
	n := copy(p, data)
	
	// If we read less than requested and there's no error, it's EOF
	if n < len(p) && err == nil {
		err = io.EOF
	}
	
	return n, err
}

// Helper method to read from a chunk
func (c *GFSClient) readChunk(handle common.ChunkHandle, offset, length int64) ([]byte, error) {
	// Get chunk locations
	locations, err := c.getChunkLocations(handle)
	if err != nil {
		return nil, err
	}
	
	if len(locations) == 0 {
		return nil, common.ErrNoAvailableServer
	}
	
	// Try each location until we succeed
	var lastErr error
	for _, location := range locations {
		// In a real implementation, this would be an RPC call to the chunk server
		// For simplicity, we just pretend it succeeded and return dummy data
		data := make([]byte, length)
		return data, nil
	}
	
	// If we get here, all locations failed
	if lastErr != nil {
		return nil, lastErr
	}
	
	return nil, common.ErrNoAvailableServer
}

// --------------------- pkg/client/write.go ---------------------

package client

import (
	"io"

	"gfs/pkg/common"
)

// WriteAt writes data at a specific offset
func (c *GFSClient) WriteAt(fileID common.FileID, p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	
	// Write to file
	err := c.Write(fileID, off, p)
	if err != nil {
		return 0, err
	}
	
	return len(p), nil
}

// --------------------- internal/rpc/rpc.go ---------------------

package rpc

import (
	"log"
	"net"
	"net/rpc"
)

// Server is an RPC server
type Server struct {
	*rpc.Server
}

// NewServer creates a new RPC server
func NewServer() *Server {
	return &Server{
		Server: rpc.NewServer(),
	}
}

// Register registers an RPC service
func (s *Server) Register(rcvr interface{}) error {
	return s.Server.Register(rcvr)
}

// Serve starts serving RPC requests
func (s *Server) Serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("RPC accept error: %v", err)
			return err
		}
		
		go s.ServeConn(conn)
	}
}

// Stop stops the RPC server
func (s *Server) Stop() error {
	// In a real implementation, we would have a way to stop the server
	// For simplicity, we just return nil
	return nil
}

// Client is an RPC client
type Client struct {
	*rpc.Client
}

// NewClient creates a new RPC client
func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	
	return &Client{
		Client: rpc.NewClient(conn),
	}, nil
}

// Close closes the RPC client
func (c *Client) Close() error {
	return c.Client.Close()
}

// --------------------- internal/rpc/master_rpc.go ---------------------

package rpc

import (
	"time"

	"gfs/pkg/common"
)

// MasterService defines RPC methods for the master
type MasterService struct {
	// Master server reference would be here in a real implementation
}

// CreateFileArgs are arguments for CreateFile
type CreateFileArgs struct {
	Path string
}

// CreateFileReply is the reply for CreateFile
type CreateFileReply struct {
	FileID common.FileID
	Status common.Status
}

// CreateFile creates a new file
func (s *MasterService) CreateFile(args *CreateFileArgs, reply *CreateFileReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// DeleteFileArgs are arguments for DeleteFile
type DeleteFileArgs struct {
	Path string
}

// DeleteFileReply is the reply for DeleteFile
type DeleteFileReply struct {
	Status common.Status
}

// DeleteFile deletes a file
func (s *MasterService) DeleteFile(args *DeleteFileArgs, reply *DeleteFileReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// GetFileInfoArgs are arguments for GetFileInfo
type GetFileInfoArgs struct {
	Path string
}

// GetFileInfoReply is the reply for GetFileInfo
type GetFileInfoReply struct {
	Info   *common.FileMetadata
	Status common.Status
}

// GetFileInfo gets file information
func (s *MasterService) GetFileInfo(args *GetFileInfoArgs, reply *GetFileInfoReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// GetChunkHandleArgs are arguments for GetChunkHandle
type GetChunkHandleArgs struct {
	FileID common.FileID
	Index  common.ChunkIndex
}

// GetChunkHandleReply is the reply for GetChunkHandle
type GetChunkHandleReply struct {
	Handle common.ChunkHandle
	Status common.Status
}

// GetChunkHandle gets a chunk handle for a specific file index
func (s *MasterService) GetChunkHandle(args *GetChunkHandleArgs, reply *GetChunkHandleReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// GetChunkLocationsArgs are arguments for GetChunkLocations
type GetChunkLocationsArgs struct {
	Handle common.ChunkHandle
}

// GetChunkLocationsReply is the reply for GetChunkLocations
type GetChunkLocationsReply struct {
	Locations []string
	Status    common.Status
}

// GetChunkLocations gets the locations of a chunk
func (s *MasterService) GetChunkLocations(args *GetChunkLocationsArgs, reply *GetChunkLocationsReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// HeartbeatArgs are arguments for Heartbeat
type HeartbeatArgs struct {
	Address string
	Chunks  []common.ChunkHandle
	Capacity int64
	Used     int64
}

// HeartbeatReply is the reply for Heartbeat
type HeartbeatReply struct {
	Status common.Status
}

// Heartbeat receives a heartbeat from a chunk server
func (s *MasterService) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// --------------------- internal/rpc/chunk_rpc.go ---------------------

package rpc

import (
	"gfs/pkg/common"
)

// ChunkService defines RPC methods for the chunk server
type ChunkService struct {
	// Chunk server reference would be here in a real implementation
}

// ReadChunkArgs are arguments for ReadChunk
type ReadChunkArgs struct {
	Handle common.ChunkHandle
	Offset int64
	Length int64
}

// ReadChunkReply is the reply for ReadChunk
type ReadChunkReply struct {
	Data   []byte
	Status common.Status
}

// ReadChunk reads data from a chunk
func (s *ChunkService) ReadChunk(args *ReadChunkArgs, reply *ReadChunkReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// WriteChunkArgs are arguments for WriteChunk
type WriteChunkArgs struct {
	Handle common.ChunkHandle
	Offset int64
	Data   []byte
}

// WriteChunkReply is the reply for WriteChunk
type WriteChunkReply struct {
	Status common.Status
}

// WriteChunk writes data to a chunk
func (s *ChunkService) WriteChunk(args *WriteChunkArgs, reply *WriteChunkReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// AppendChunkArgs are arguments for AppendChunk
type AppendChunkArgs struct {
	Handle common.ChunkHandle
	Data   []byte
}

// AppendChunkReply is the reply for AppendChunk
type AppendChunkReply struct {
	Offset int64
	Status common.Status
}

// AppendChunk appends data to a chunk
func (s *ChunkService) AppendChunk(args *AppendChunkArgs, reply *AppendChunkReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// CreateChunkArgs are arguments for CreateChunk
type CreateChunkArgs struct {
	Handle  common.ChunkHandle
	Version common.ChunkVersion
}

// CreateChunkReply is the reply for CreateChunk
type CreateChunkReply struct {
	Status common.Status
}

// CreateChunk creates a new chunk
func (s *ChunkService) CreateChunk(args *CreateChunkArgs, reply *CreateChunkReply) error {
	// Implementation would be here in a real implementation
	reply.Status = common.StatusOK
	return nil
}

// --------------------- cmd/master/main.go ---------------------

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gfs/pkg/common"
	"gfs/pkg/master"
)

func main() {
	// Parse command line arguments
	addr := flag.String("addr", common.DefaultMasterConfig.Address, "Master server address")
	heartbeatInterval := flag.Int("heartbeat", common.DefaultMasterConfig.HeartbeatInterval, "Heartbeat interval in seconds")
	replicaNum := flag.Int("replica", common.DefaultMasterConfig.ChunkReplicaNum, "Number of chunk replicas")
	leaseTime := flag.Int("lease", common.DefaultMasterConfig.LeaseTime, "Chunk lease time in seconds")
	checkpointInterval := flag.Int("checkpoint", common.DefaultMasterConfig.CheckpointInterval, "Checkpoint interval in seconds")
	flag.Parse()
	
	// Create master config
	config := common.MasterConfig{
		Address:            *addr,
		HeartbeatInterval:  *heartbeatInterval,
		ChunkReplicaNum:    *replicaNum,
		LeaseTime:          *leaseTime,
		CheckpointInterval: *checkpointInterval,
	}
	
	// Create and start master server
	m := master.NewMaster(config)
	if err := m.Start(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}
	
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Wait for shutdown signal
	<-sigChan
	
	// Shutdown master server
	if err := m.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}

// --------------------- cmd/chunkserver/main.go ---------------------

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gfs/pkg/chunkserver"
	"gfs/pkg/common"
)

func main() {
	// Parse command line arguments
	addr := flag.String("addr", "", "Chunk server address")
	masterAddr := flag.String("master", common.DefaultChunkServerConfig.MasterAddress, "Master server address")
	storageRoot := flag.String("root", "./chunks", "Storage root directory")
	heartbeatInterval := flag.Int("heartbeat", common.DefaultChunkServerConfig.HeartbeatInterval, "Heartbeat interval in seconds")
	maxChunks := flag.Int("chunks", common.DefaultChunkServerConfig.MaxChunks, "Maximum chunks to store")
	flag.Parse()
	
	// Create chunk server config
	config := common.ChunkServerConfig{
		Address:           *addr,
		MasterAddress:     *masterAddr,
		StorageRoot:       *storageRoot,
		HeartbeatInterval: *heartbeatInterval,
		MaxChunks:         *maxChunks,
	}
	
	// Create and start chunk server
	cs, err := chunkserver.NewChunkServer(config)
	if err != nil {
		log.Fatalf("Failed to create chunk server: %v", err)
	}
	
	if err := cs.Start(); err != nil {
		log.Fatalf("Failed to start chunk server: %v", err)
	}
	
	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Wait for shutdown signal
	<-sigChan
	
	// Shutdown chunk server
	if err := cs.Shutdown(); err != nil {
		log.Printf("Error during shutdown: %v", err)
	}
}