package chunkserver

import (
	"context"
	"log"
	"net"
	netrpc "net/rpc"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// ChunkServer represents a chunk server
type ChunkServer struct {
	config         common.ChunkServerConfig
	storageManager *StorageManager
	chunks         map[common.ChunkUsername]*Chunk
	mutations      map[common.ChunkUsername][]common.Mutation
	chunkMutex     sync.RWMutex
	mutationMutex  sync.Mutex
	rpcServer      *netrpc.Server
	shutdown       chan struct{}
	isHealthy      bool
	nextWriteID    common.WriteID // serial number for primary writes
	writeIDMutex   sync.Mutex
	primaryChunks  map[common.ChunkUsername]bool // chunks this server is primary for
	primaryMutex   sync.RWMutex
}

// NewChunkServer creates a new chunk server
func NewChunkServer(config common.ChunkServerConfig) (*ChunkServer, error) {
	return &ChunkServer{
		config:         config,
		storageManager: NewStorageManager(config.StorageRoot),
		chunks:         make(map[common.ChunkUsername]*Chunk),
		mutations:      make(map[common.ChunkUsername][]common.Mutation),
		primaryChunks:  make(map[common.ChunkUsername]bool),
		shutdown:       make(chan struct{}),
	}, nil
}

// Start initializes and runs the chunk server
func (cs *ChunkServer) Start() error {
	// load chunks from disk
	usernames, err := cs.storageManager.LoadChunks()
	if err != nil {
		return err
	}

	// register chunks in memory
	for _, username := range usernames {
		chunk, err := cs.loadChunk(username)
		if err != nil {
			log.Printf("Failed to load chunk %d: %v", username, err)
			continue
		}
		cs.chunkMutex.Lock()
		cs.chunks[username] = chunk
		cs.chunkMutex.Unlock()
	}

	// start RPC server
	listener, err := net.Listen("tcp", cs.config.Address)
	if err != nil {
		return err
	}

	cs.rpcServer = netrpc.NewServer()
	cs.registerRPCMethods()

	go cs.Serve(listener)

	// start background tasks
	go cs.SendHeartbeats()
	go cs.applyMutations()

	cs.isHealthy = true

	// register with master
	if err := cs.registerWithMaster(); err != nil {
		log.Printf("Failed to register with master: %v", err)
		// continue anyway
	}

	log.Printf("ChunkServer started at %s", cs.config.Address)
	return nil
}

// Serve accepts RPC connections
func (cs *ChunkServer) Serve(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-cs.shutdown:
				return
			default:
				log.Printf("Accept error: %v", err)
				continue
			}
		}
		go cs.rpcServer.ServeConn(conn)
	}
}

// Shutdown stops the chunk server
func (cs *ChunkServer) Shutdown() error {
	if !cs.isHealthy {
		return nil
	}
	close(cs.shutdown)
	cs.isHealthy = false
	return nil
}

// CreateChunk creates a new chunk
func (cs *ChunkServer) CreateChunk(username common.ChunkUsername, version common.ChunkVersion) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	if _, exists := cs.chunks[username]; exists {
		return nil
	}

	chunk := NewChunk(username, version)

	if err := cs.storageManager.CreateChunk(username); err != nil {
		return err
	}

	if err := cs.storageManager.UpdateMetadata(username, version, 0); err != nil {
		return err
	}

	cs.chunks[username] = chunk
	return nil
}

// DeleteChunk deletes a chunk
func (cs *ChunkServer) DeleteChunk(username common.ChunkUsername) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	if err := cs.storageManager.DeleteChunk(username); err != nil {
		return err
	}
	delete(cs.chunks, username)
	return nil
}

// ReadChunk reads data from a chunk
func (cs *ChunkServer) ReadChunk(username common.ChunkUsername, offset uint64, length uint64) ([]byte, error) {
	cs.chunkMutex.RLock()
	_, ok := cs.chunks[username]
	cs.chunkMutex.RUnlock()

	if !ok {
		return nil, common.ErrChunkNotFound
	}

	if offset >= common.ChunkSize {
		return nil, common.ErrInvalidOffset
	}

	// cap length to end of chunk
	if offset+length > common.ChunkSize {
		length = common.ChunkSize - offset
	}

	return cs.storageManager.readChunk(username, offset, length)
}

// WriteChunk writes data to a chunk
func (cs *ChunkServer) WriteChunk(username common.ChunkUsername, offset uint64, data []byte) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	if offset >= common.ChunkSize {
		return common.ErrInvalidOffset
	}

	if offset+uint64(len(data)) > common.ChunkSize {
		return common.ErrInvalidArgument
	}

	if err := cs.storageManager.WriteChunk(username, offset, data); err != nil {
		return err
	}

	newSize := offset + uint64(len(data))
	if newSize > chunk.Size {
		chunk.Size = newSize
		if err := cs.storageManager.UpdateMetadata(username, chunk.Version, newSize); err != nil {
			log.Printf("Failed to update metadata: %v", err)
		}
	}
	return nil
}

// AppendChunk appends data to a chunk
func (cs *ChunkServer) AppendChunk(username common.ChunkUsername, data []byte) (uint64, error) {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return 0, common.ErrChunkNotFound
	}

	if chunk.Size+uint64(len(data)) > common.ChunkSize {
		return 0, common.ErrInvalidOffset
	}

	if err := cs.storageManager.WriteChunk(username, chunk.Size, data); err != nil {
		return 0, err
	}

	chunk.Size += uint64(len(data))
	cs.chunks[username] = chunk

	if err := cs.storageManager.UpdateMetadata(username, chunk.Version, chunk.Size); err != nil {
		log.Printf("Failed to update metadata: %v", err)
	}

	return chunk.Size, nil
}

// GetChunkData returns the metadata and data for a chunk
func (cs *ChunkServer) GetChunkData(username common.ChunkUsername) (*common.ChunkData, error) {
	info, err := cs.GetChunkInfo(username)
	if err != nil {
		return nil, err
	}

	data, err := cs.storageManager.readChunk(username, 0, info.Size)
	if err != nil {
		return nil, err
	}

	return &common.ChunkData{Metadata: *info, Data: data}, nil
}

// GetChunkInfo returns metadata for a chunk
func (cs *ChunkServer) GetChunkInfo(username common.ChunkUsername) (*common.ChunkMetadata, error) {
	cs.chunkMutex.RLock()
	defer cs.chunkMutex.RUnlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return nil, common.ErrChunkNotFound
	}

	return &common.ChunkMetadata{
		Username: chunk.Username,
		Version:  chunk.Version,
		Size:     chunk.Size,
	}, nil
}

// GetBlockChecksum returns the checksum of a block within a chunk
func (cs *ChunkServer) GetBlockChecksum(username common.ChunkUsername, blockIndex uint64) (uint32, error) {
	return cs.storageManager.GetBlockChecksum(username, blockIndex)
}

// GetChunkChecksum returns the checksum of an entire chunk
func (cs *ChunkServer) GetChunkChecksum(username common.ChunkUsername) (uint32, error) {
	cs.chunkMutex.RLock()
	chunk, ok := cs.chunks[username]
	cs.chunkMutex.RUnlock()

	if !ok {
		return 0, common.ErrChunkNotFound
	}

	data, err := cs.storageManager.readChunk(username, 0, chunk.Size)
	if err != nil {
		return 0, err
	}

	return common.Checksum(data), nil
}

// ApplyMutation queues a mutation for a chunk
func (cs *ChunkServer) ApplyMutation(username common.ChunkUsername, mutation common.Mutation) error {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()

	cs.mutations[username] = append(cs.mutations[username], mutation)
	return nil
}

// SendHeartbeats periodically sends heartbeats to the master
func (cs *ChunkServer) SendHeartbeats() {
	ticker := time.NewTicker(time.Duration(cs.config.HeartbeatInterval) * time.Second)
	defer ticker.Stop()

	var masterClient *netrpc.Client
	var dialErr error

	defer func() {
		if masterClient != nil {
			masterClient.Close()
		}
	}()

	for {
		select {
		case <-ticker.C:
			if masterClient == nil {
				masterClient, dialErr = netrpc.Dial("tcp", cs.config.MasterAddress)
				if dialErr != nil {
					log.Printf("Failed to connect to master: %v", dialErr)
					continue
				}
			}

			cs.chunkMutex.RLock()
			handles := make([]common.ChunkUsername, 0, len(cs.chunks))
			var reports []common.ChunkReport
			for handle, chunk := range cs.chunks {
				handles = append(handles, handle)
				reports = append(reports, common.ChunkReport{
					Username: handle,
					Version:  chunk.Version,
				})
			}
			cs.chunkMutex.RUnlock()

			cs.primaryMutex.RLock()
			var renewals []common.ChunkUsername
			for handle := range cs.primaryChunks {
				renewals = append(renewals, handle)
			}
			cs.primaryMutex.RUnlock()

			capacity, used, statsErr := cs.storageManager.GetStats()
			if statsErr != nil {
				log.Printf("Failed to get storage stats: %v", statsErr)
			}

			args := &common.HeartbeatRequest{
				Address:       cs.config.Address,
				Chunks:        handles,
				ChunkReports:  reports,
				Capacity:      capacity,
				UsedSpace:      used,
				LeaseRenewals: renewals,
			}
			reply := &common.HeartbeatReply{}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			done := make(chan error, 1)
			go func() {
				done <- masterClient.Call("Master.HandleHeartbeat", args, reply)
			}()

			select {
			case callErr := <-done:
				cancel()
				if callErr != nil {
					log.Printf("Heartbeat failed: %v", callErr)
					masterClient.Close()
					masterClient = nil
				} else if reply.Status != common.StatusOK {
					log.Printf("Heartbeat rejected by master: status %v", reply.Status)
				} else {
					log.Printf("Heartbeat OK: %d chunks, %d/%d bytes used", len(handles), used, capacity)
					if len(reply.ChunksToDelete) > 0 {
						go cs.deleteChunks(reply.ChunksToDelete)
					}
					if len(reply.ChunksToReplicate) > 0 {
						go cs.replicateChunks(reply.ChunksToReplicate)
					}
					// Update primary status based on lease renewal responses
					if len(renewals) > 0 {
						renewedSet := make(map[common.ChunkUsername]bool)
						for _, h := range reply.RenewedLeases {
							renewedSet[h] = true
						}
						cs.primaryMutex.Lock()
						for h := range cs.primaryChunks {
							if !renewedSet[h] {
								delete(cs.primaryChunks, h)
							}
						}
						cs.primaryMutex.Unlock()
					}
				}
			case <-ctx.Done():
				cancel()
				log.Printf("Heartbeat timed out")
				masterClient.Close()
				masterClient = nil
			}

		case <-cs.shutdown:
			return
		}
	}
}

func (cs *ChunkServer) applyMutations() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs.ProcessPendingMutations()
		case <-cs.shutdown:
			return
		}
	}
}

// ProcessPendingMutations processes all queued mutations
func (cs *ChunkServer) ProcessPendingMutations() {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()

	for username, mutations := range cs.mutations {
		if len(mutations) == 0 {
			continue
		}

		for _, mutation := range mutations {
			var err error
			switch mutation.Type {
			case common.MutationWrite:
				err = cs.WriteChunk(username, mutation.Offset, mutation.Data)
			case common.MutationDelete:
				err = cs.DeleteChunk(username)
			case common.MutationAppend:
				_, err = cs.AppendChunk(username, mutation.Data)
			}
			if err != nil {
				log.Printf("Failed to apply mutation: %v", err)
			}
		}

		delete(cs.mutations, username)
	}
}

func (cs *ChunkServer) loadChunk(username common.ChunkUsername) (*Chunk, error) {
	version, size, err := cs.storageManager.ReadMetadata(username)
	if err != nil {
		return nil, err
	}

	chunk := NewChunk(username, version)
	chunk.Size = size
	return chunk, nil
}

func (cs *ChunkServer) registerRPCMethods() {
	if err := cs.rpcServer.Register(cs); err != nil {
		log.Printf("Failed to register RPC methods: %v", err)
	}
}

func (cs *ChunkServer) registerWithMaster() error {
	client, err := netrpc.Dial("tcp", cs.config.MasterAddress)
	if err != nil {
		return err
	}
	defer client.Close()

	args := struct{ Address string }{Address: cs.config.Address}
	var reply struct{ Status common.Status }
	return client.Call("Master.RegisterChunkServer", args, &reply)
}

func (cs *ChunkServer) deleteChunks(usernames []common.ChunkUsername) {
	for _, username := range usernames {
		if err := cs.DeleteChunk(username); err != nil {
			log.Printf("Failed to delete chunk %d: %v", username, err)
		}
	}
}

func (cs *ChunkServer) replicateChunks(usernames []common.ChunkUsername) {
	// TODO: implement chunk replication
	_ = usernames
}
