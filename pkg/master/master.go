package master

import (
	"log"
	"net"
	"context"
	"sync"
	"time"

	"github.com/sauravfouzdar/internal/rpc"
	"github.com/sauravfouzdar/pkg/common"
)

// Master manages entire "bucket" cluster
type Master struct {
	config			common.MasterConfig
	metadata		*MetadataManager
	leaseManager	*LeaseManager

	// Chunk server management
	chunkServers	map[string]*ChunkServerInfo
	chunkServerMutext	sync.RWMutex


	// Server state
	isHealthy		bool
	shutdown		chan struct{} // why using struct{} instead of bool? - Hint: channel is used for signaling

	// RPC server
	rpcServer		*rpc.Server
}

// ChunkServerInfo contains information about a chunk server
type ChunkServerInfo struct {
	Address		string
	LastHeard	time.Time
	Chunks		[]common.ChunkUsername
	Available	bool
	Capacity	uint64 // total storage capacity in bytes
	UsedSpace	uint64 // used storage space in bytes
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
func (m *Master) Shutdown() {
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

// HandleHeartbeat handles chunk server heartbeat
func (m *Master) HandleHeartbeat(address string, chunks []common.Username, capacity, used int64) (*HeartbeatResponse, error) {
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

// registerRPCMethods registers all the RPC methods
func (m *Master) registerRPCMethods() {
	m.rpcServer.Register(&MasterService{master: m})
}

