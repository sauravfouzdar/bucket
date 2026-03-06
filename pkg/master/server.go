package master

import (
	"fmt"
	"log"
	"net"
	netrpc "net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// Master manages the entire bucket cluster
type Master struct {
	namespace     *Namespace
	metadata      *MetadataManager
	leaseManager  *LeaseManager
	chunkServers  map[string]*ChunkServerInfo // keyed by address
	opLog         *OperationLog
	mu            sync.RWMutex
	chunkServerMu sync.RWMutex
	shutdown      chan struct{}
	isHealthy     bool
	rpcServer     *netrpc.Server
	config        common.MasterConfig
}

// FileInfo extends common FileMetadata with master-specific info
type FileInfo struct {
	common.FileMetadata
}

// ChunkInfo extends common ChunkMetadata with lease info
type ChunkInfo struct {
	common.ChunkMetadata
	PrimaryExpiration time.Time
	Primary           common.ServerID
}

// ChunkServerInfo contains information about a registered chunk server
type ChunkServerInfo struct {
	ID            common.ServerID
	Address       string
	LastHeartbeat time.Time
	Chunks        []common.ChunkUsername
	Capacity      uint64
	UsedSpace     uint64
	Available     bool
}

// Lease represents a primary lease granted to a chunk server
type Lease struct {
	Primary     common.ServerID
	Secondaries []common.ServerID
	Expiration  time.Time
}

// OperationLog records mutations for crash recovery
type OperationLog struct {
	entries        []LogEntry
	lastCheckpoint int
	logFile        *os.File
}

// LogEntry represents a single operation log entry
type LogEntry struct {
	Timestamp   time.Time            `json:"timestamp"`
	Operation   string               `json:"operation"`
	Path        string               `json:"path,omitempty"`
	FileID      common.FileID        `json:"file_id,omitempty"`
	ChunkHandle common.ChunkHandle   `json:"chunk_handle,omitempty"`
	ChunkIndex  common.ChunkIndex    `json:"chunk_index,omitempty"`
	Version     common.ChunkVersion  `json:"version,omitempty"`
}

// Namespace holds the file system tree
type Namespace struct {
	mu   sync.RWMutex
	root *NamespaceNode
}

// NamespaceNode is a node in the namespace tree
type NamespaceNode struct {
	Name        string
	IsDirectory bool
	FileID      common.FileID
	Children    map[string]*NamespaceNode
}

// NewMaster creates a new Master
func NewMaster(config common.MasterConfig) *Master {
	return &Master{
		config:       config,
		metadata:     NewMetadataManager(),
		leaseManager: NewLeaseManager(time.Duration(config.LeaseTimeout) * time.Second),
		chunkServers: make(map[string]*ChunkServerInfo),
		shutdown:     make(chan struct{}),
		namespace:    NewNamespace(),
	}
}

// Start starts the master server
func (m *Master) Start() error {
	if err := m.metadata.LoadFromDisk(); err != nil {
		log.Printf("Failed to load metadata from disk: %v", err)
	}

	logPath := filepath.Join("./metadata", "oplog.jsonl")
	opLog, err := NewOperationLog(logPath)
	if err != nil {
		return fmt.Errorf("failed to open operation log: %w", err)
	}
	m.opLog = opLog
	if err := m.opLog.RecoverFromLog(m); err != nil {
		log.Printf("WAL recovery warning: %v", err)
	}

	listener, err := net.Listen("tcp", m.config.Address)
	if err != nil {
		return err
	}

	m.rpcServer = netrpc.NewServer()
	m.registerRPCMethods()

	go m.serveRPC(listener)

	go m.periodicCheckpoint()
	go m.monitorChunkServers()
	go m.periodicLeaseCleanup()

	m.isHealthy = true
	log.Printf("Master started at %s", m.config.Address)
	return nil
}

// Shutdown stops the master gracefully
func (m *Master) Shutdown() error {
	if !m.isHealthy {
		return nil
	}
	close(m.shutdown)
	if m.opLog != nil {
		if err := m.opLog.Checkpoint(m); err != nil {
			log.Printf("checkpoint on shutdown failed: %v", err)
		}
		m.opLog.Close()
	} else {
		if err := m.metadata.SaveToDisk(); err != nil {
			log.Printf("Failed to save metadata to disk: %v", err)
		}
	}
	m.isHealthy = false
	return nil
}

// CreateFile creates a new file and returns its ID
func (m *Master) CreateFile(path string) (common.FileID, error) {
	fileID, err := m.metadata.CreateFile(path)
	if err != nil {
		return "", err
	}
	if nsErr := m.namespace.CreateFile(path, fileID); nsErr != nil {
		log.Printf("namespace sync warning for %s: %v", path, nsErr)
	}
	return fileID, nil
}

// pickChunkServers returns up to n available chunkserver addresses
func (m *Master) pickChunkServers(n int) []string {
	m.chunkServerMu.RLock()
	defer m.chunkServerMu.RUnlock()
	addrs := make([]string, 0, n)
	for addr, info := range m.chunkServers {
		if info.Available {
			addrs = append(addrs, addr)
			if len(addrs) >= n {
				break
			}
		}
	}
	return addrs
}

// DeleteFile deletes a file
func (m *Master) DeleteFile(path string) error {
	return m.metadata.DeleteFile(path)
}

// GetFileInfo returns metadata for a file
func (m *Master) GetFileInfo(path string) (*common.FileMetadata, error) {
	return m.metadata.GetFileMetadata(path)
}

// GetChunkHandle returns the chunk handle for a given file and chunk index
func (m *Master) GetChunkHandle(path string, chunkIndex int) (common.ChunkHandle, error) {
	fileID, err := m.namespace.LookupFile(path)
	if err != nil {
		return 0, err
	}
	return m.metadata.GetChunkUsername(fileID, common.ChunkIndex(chunkIndex))
}

// GetChunkLocations returns the server addresses for a chunk
func (m *Master) GetChunkLocations(handle common.ChunkHandle) ([]common.ChunkLocation, error) {
	locations, err := m.metadata.GetChunkLocations(handle)
	if err != nil {
		return nil, err
	}
	result := make([]common.ChunkLocation, len(locations))
	for i, addr := range locations {
		result[i] = common.ChunkLocation{ServerAddress: addr}
	}
	return result, nil
}

// AllocateChunk allocates a new chunk for a file
func (m *Master) AllocateChunk(path string, chunkIndex int) (common.ChunkHandle, []common.ChunkLocation, error) {
	fileID, err := m.namespace.LookupFile(path)
	if err != nil {
		return 0, nil, err
	}
	handle, err := m.metadata.CreateChunk(fileID, common.ChunkIndex(chunkIndex))
	if err != nil {
		return 0, nil, err
	}
	return handle, nil, nil
}

// ReportChunk updates chunk info from a chunk server
func (m *Master) ReportChunk(serverID common.ServerID, handle common.ChunkHandle, version common.ChunkVersion) error {
	m.metadata.AddChunkLocation(handle, "")
	return nil
}

// HandleHeartbeat handles a heartbeat from a chunk server (net/rpc compatible)
func (m *Master) HandleHeartbeat(args *common.HeartbeatRequest, reply *common.HeartbeatReply) error {
	m.chunkServerMu.Lock()
	defer m.chunkServerMu.Unlock()

	info, exists := m.chunkServers[args.Address]
	if !exists {
		info = &ChunkServerInfo{
			Address:       args.Address,
			LastHeartbeat: time.Now(),
			Chunks:        args.Chunks,
			Capacity:      args.Capacity,
			UsedSpace:     args.UsedSpace,
			Available:     true,
		}
		m.chunkServers[args.Address] = info
	} else {
		info.LastHeartbeat = time.Now()
		info.Chunks = args.Chunks
		info.Capacity = args.Capacity
		info.UsedSpace = args.UsedSpace
		info.Available = true
	}

	// Process chunk reports with stale version detection
	if len(args.ChunkReports) > 0 {
		for _, report := range args.ChunkReports {
			chunkMeta, err := m.metadata.GetChunkMetadata(report.Username)
			if err != nil {
				continue
			}
			if report.Version < chunkMeta.Version {
				// Stale replica -- schedule for deletion
				reply.ChunksToDelete = append(reply.ChunksToDelete, report.Username)
				log.Printf("Stale chunk %d on %s: has v%d, current v%d",
					report.Username, args.Address, report.Version, chunkMeta.Version)
			} else {
				_ = m.metadata.AddChunkLocation(report.Username, args.Address)
			}
		}
	} else {
		// Fallback: old-style heartbeat without version info
		for _, chunk := range args.Chunks {
			_ = m.metadata.AddChunkLocation(chunk, args.Address)
		}
	}

	// Process lease renewals
	for _, username := range args.LeaseRenewals {
		leaseInfo := m.leaseManager.GetLease(username)
		if leaseInfo != nil && leaseInfo.Primary == args.Address {
			if _, err := m.leaseManager.RenewLease(username); err == nil {
				reply.RenewedLeases = append(reply.RenewedLeases, username)
			}
		}
	}

	reply.Status = common.StatusOK
	return nil
}

// RegisterChunkServer registers a new chunk server
func (m *Master) RegisterChunkServer(args *struct{ Address string }, reply *struct{ Status common.Status }) error {
	m.chunkServerMu.Lock()
	defer m.chunkServerMu.Unlock()

	if _, exists := m.chunkServers[args.Address]; exists {
		reply.Status = common.StatusOK
		return nil
	}

	m.chunkServers[args.Address] = &ChunkServerInfo{
		Address:       args.Address,
		LastHeartbeat: time.Now(),
		Chunks:        []common.ChunkUsername{},
		Available:     true,
	}

	log.Printf("Chunk server registered: %s", args.Address)
	reply.Status = common.StatusOK
	return nil
}

func (m *Master) registerRPCMethods() {
	if err := m.rpcServer.Register(m); err != nil {
		log.Printf("Failed to register RPC methods: %v", err)
	}
}

func (m *Master) serveRPC(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-m.shutdown:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}
		go m.rpcServer.ServeConn(conn)
	}
}

func (m *Master) periodicCheckpoint() {
	ticker := time.NewTicker(time.Duration(m.config.CheckpointInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if m.opLog != nil {
				if err := m.opLog.Checkpoint(m); err != nil {
					log.Printf("Failed to checkpoint: %v", err)
				}
			} else {
				if err := m.metadata.SaveToDisk(); err != nil {
					log.Printf("Failed to save metadata: %v", err)
				}
			}
		case <-m.shutdown:
			return
		}
	}
}

func (m *Master) monitorChunkServers() {
	ticker := time.NewTicker(time.Duration(m.config.HeartbeatInterval) * time.Second)
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

func (m *Master) periodicLeaseCleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.leaseManager.CleanExpired()
		case <-m.shutdown:
			return
		}
	}
}

func (m *Master) detectDeadChunkServers() {
	m.chunkServerMu.Lock()
	defer m.chunkServerMu.Unlock()

	deadline := time.Duration(m.config.HeartbeatInterval*3) * time.Second
	for address, info := range m.chunkServers {
		if time.Since(info.LastHeartbeat) > deadline {
			log.Printf("Chunk server %s is dead", address)
			for _, chunk := range info.Chunks {
				m.metadata.RemoveChunkLocation(chunk, address)
			}
			delete(m.chunkServers, address)
		}
	}
}
