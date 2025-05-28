package master

import (
	"log"
	"net"
	"context"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/internal/rpc"
	"github.com/sauravfouzdar/bucket/pkg/common"
)

// Master manages entire "bucket" cluster
type Master struct {
	// Namespace management
	namespace	*Namespace

	// Metadata management
	files		map[common.FileID]*FileInfo
	chunks		map[common.ChunkHandle]*ChunkInfo

	// Server management
	chunkServers	map[common.ServerID]*ChunkInfo

	// Lease management
	leases		map[common.ChunkHandle]*Lease

	// Operation log
	opLog		*OperationLog

	// Mutexes for concurrent access
	mu		sync.RWMutex
	namespaceMu	sync.RWMutex

	// Configuration
	config		Config
}

// FileInfo extends the common FileMetadata with master-specific information
type FileInfo struct {
	common.FileMetadata
}

// ChunkInfo extends the common ChunkMetadata with master-specific information
type ChunkInfo struct {
	common.ChunkMetadata
	PrimaryExpiration	time.Time // When primary lease expires
	Primary		common.ServerID    // Current primary, if any
}

// ChunkServerInfo contains information about a chunk server
type ChunkServerInfo struct {
	ID				common.ServerID
	Address			string
	LastHeartbeat	time.Time
	Chunks			[]common.ChunkUsername
	Capacity		uint64 // total storage capacity in bytes
	UsedSpace		uint64 // used storage space in bytes
}

// Lease represents a lease granted to a primary chunk server
type Lease struct {
	Primary		common.ServerID
	Secondaries	[]common.ServerID
	Expiration	time.Time // When primary lease expires
}

// OperationLog represents the operation log
type OperationLog struct {
	entries	[]LogEntry
	lastCheckpoint int
	logFile *os.File
}
// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp	time.Time
	Operation	string
	Path		string
	ChunkHandle	common.ChunkHandle
	Version		common.ChunkVersion
}

type Namespace struct {
	root *NamespaceNode
}

type NamespaceNode struct {
	Name string
	IsDirectory bool
	FileID common.FileID // valid if !IsDirectory
	Children map[string]*NamespaceNode // valid if IsDirectory
}

func NewMaster(config common.MasterConfig) *Master {
	m := &Master{
		config:			config,
		metadata:		NewMetadataManager(),
		leaseManager:	NewLeaseManager(time.Duration(config.LeaseTimeout) * time.Second),
		chunkServers:	make(map[string]*ChunkServerInfo),
		shutdown:		make(chan struct{}),
	}

	return m
}

// Start starts the master
func (m *Master) Start() error {
	// load metadata from disk(recovery)
	if err := m.metadata.LoadFromDisk(); err != nil {
		log.Printf("Failed to load metadata from disk: %v", err)
		// continue anyway
	}

	// start RPC server
	listener, err := net.Listen("tcp", m.config.Address)
	if err != nil {
		return err
	}

	m.rpcServer = rpc.NewServer()
	// register master service
	m.registerRPCMethods()

	go m.rpcServer.Serve(listener)

	// start background tasks
	go m.periodicCheckpoint()
	go m.monitorChunkServers()

	m.isHealthy = true

	log.Printf("Master started at %s", m.config.Address)
	return nil
}

// Shutdown stops the master
func (m *Master) Shutdown() error {
	// if already unhealthy
	if !m.isHealthy {
		return nil
	}
	close(m.shutdown)

	// perform checkpoint before shutdown
	if err := m.metadata.SaveToDisk(); err != nil {
		log.Printf("Failed to save metadata to disk: %v", err)
	}
	m.isHealthy = false
	return m.rpcServer.Stop()
}

// CreateFile creates a new file
func (m *Master) CreateFile(path string) error {
}

// DeleteFile deletes a file
func (m *Master) DeleteFile(path string) error {
}

// GetFileInfo returns metadata for a file
func (m *Master) GetFileInfo(path string) (*common.FileMetadata, error){
}

// GetChunkHandle returns the chunk handle for a given file and chunk index
func (m *Master) GetChunkHandle(path string, chunkIndex int) (common.ChunkHandle, error) {
}
// GetChunkLocations returns the locations of a chunk
func (m *Master) GetChunkLocations(handle common.ChunkHandle) ([]common.ChunkLocation, error) {
}

// AllocateChunk allocates a new chunk for a file
func (m *Master) AllocateChunk(path string, chunkIndex int) (common.ChunkHandle, []common.ChunkLocation, error) {
}
// ReportChunk reports a chunk from a chunk server
func (m *Master) ReportChunk(serverID common.ServerID, handle common.ChunkHandle, version common.Version) error {
}


// HandleHeartbeat handles chunk server heartbeat
func (m *Master) HeartbeatHandler(address string, chunks []common.Username, capacity, used int64) (*HeartbeatResponse, error) {
	m.chunkServerMutext.Lock()
	defer m.chunkServerMutext.Unlock()

	info, exists := m.chunkServers[address]
	if !exists {
		info = &ChunkServerInfo{
			Address:	address,
			LastHeard:	time.Now(),
			Chunks:		chunks,
			Available:	true,
			Capacity:	capacity,
			UsedSpace:	used,
		}
		m.chunkServers[address] = info
	} else {
		info.LastHeard = time.Now()
		info.Chunks = chunks
		info.Capacity = capacity
		info.UsedSpace = used
		info.Available = true
	}
}

// RegisterChunkServer registers a new chunk server
func (m *Master) RegisterChunkServer(serverID common.ServerID,address string) error {
	m.chunkServerMutext.Lock()
	defer m.chunkServerMutext.Unlock()

	if _, exists := m.chunkServers[serverID]; exists {
		return common.ErrChunkServerAlreadyExists
	}

	m.chunkServers[serverID] = &ChunkServerInfo{
		ID: serverID,
		Address: address,
		LastHeartbeat: time.Now(),
		Chunks: []common.ChunkUsername{},
	}

	log.Printf("Chunk server %s registered with ID %d", address, serverID)
	return nil
}
// periodicCheckpoint periodically saves metadata to disk
func (m *Master) periodicCheckpoint() {
	ticker := time.NewTicker(time.Duration(m.config.CheckpointInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := m.metadata.SaveToDisk(); err != nil {
				log.Printf("Failed to save metadata to disk: %v", err)
			}
		case <-m.shutdown:
			ticker.Stop()
			return
		}
	}
}

// detectDeadChunkServers detects dead chunk servers
func (m *Master) detectDeadChunkServers() {
	m.chunkServerMutext.Lock()
	defer m.chunkServerMutext.Unlock()

	for address, info := range m.chunkServers {
		if time.Since(info.LastHeard) > time.Duration(m.config.HeartbeatInterval)*time.Second {
			log.Printf("Chunk server %s is dead", address)
			// mark chunks as unavailable
			for _, chunk := range info.Chunks {
				m.metadata.MarkChunkUnavailable(chunk)
			}
			delete(m.chunkServers, address)
		}
	}
}

// monitorChunkServers periodically checks the health of chunk servers
func (m *Master) monitorChunkServers() {
	ticker := time.NewTicker(time.Duration(m.config.HeartbeatInterval) * time.Second)
	for {
		select {
		case <-ticker.C:
			m.detectDeadChunkServers()
		case <-m.shutdown:
			ticker.Stop()
			return
		}
	}
}


