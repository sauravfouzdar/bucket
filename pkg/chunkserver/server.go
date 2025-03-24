package chunkserver

import (
	"context"
	"time"

	"github.com/sauravfouzdar/pkg/common"
)

// ChunkServer represents a chunk server
type ChunkServer struct {
	config 		common.ChunkServerConfig

	storageManager 	*StorageManager

	chunks 		map[common.ChunkUsername]*Chunk
	chunkMutex 		sync.RWMutex

	// mutation queue
	mutations 	map[common.ChunkUsername][]common.Mutation
	mutationMutex 	sync.RWMutex

	// Server state
	isHealthy 	bool
	shutdown 	chan struct{}

	// RPC server
	rpcServer 	*rpc.Server
}

// NewChunkServer creates a new chunk server
func NewChunkServer(config common.ChunkServerConfig) *ChunkServer {
	return &ChunkServer{
		config: config,
		storageManager: NewStorageManager(config.StorageRoot),
		chunks: make(map[common.ChunkUsername]*Chunk),
		mutations: make(map[common.ChunkUsername][]common.Mutation),
		shutdown: make(chan struct{}),
	}, nil
}

// Start initializes the chunk server
func (cs *ChunkServer) Start() error {

	// load chunks from disk
	usernames, err := cs.storageManager.LoadChunks()
	if err != nil {
		return err
	}

	// register chunks in memory
	for _, username := range usernames {
		chunk, err := cs.loadchunk(username)
		if err != nil {
			log.Printf("Failed to load chunk %d: %v", username, err)
		}

		cs.chunkMutex.Lock()
		cs.chunks[username] = chunk
		cs.chunkMutex.Unlock()
	}

	// start RPC server
	listner, err := net.Listen("tcp", cs.config.Address)
	if err != nil {
		return err
	}

	cs.rpcServer = rpc.NewServer()
	// Register rpc method
	cs.registerRPCMethods()

	go cs.rpcServer.Serve(listner)

	// start heartbeat
	go cs.sendHeartbeats()
	go cs.applyMutations()

	cs.isHealthy = true

	// Register with master
	if err := cs.registerWithMaster(); err != nil {
		log.Printf("Failed to register with master: %v", err)
		// continue anyway
	}

	log.Printf("Chunkserver started at %s", cs.config.Address)
	return nil
}

func (cs *ChunkServer) Shutdown() error {
	if !cs.isHealthy {
		return nil
	}

	close(cs.shutdown)

	// wait group (tbd)
	time.Sleep(5* time.Second)

	cs.isHealthy = false
	return cs.rpcServer.Stop()
}

// CreateChunk create a new chunk
func (cs *ChunkServer) CreateChunk(username common.ChunkUsername, version common.ChunkVersion) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	//  check if chunk already exists
	if _, exists := cs.chunks[username]; exists {
		return nil
	}

	// create chunk
	chunk := NewChunk(handle, version)

	// write to disk
	err := cs.storageManager.CreateChunk(username)
	if err != nil {
		return err
	}

	// update metadata
	err := cs.storageManager.UpdateMetadata(username, version, 0)
	if err != nil {
		return err
	}
	cs.chunks[hangle] = chunk
	return nil
}

// ReadChunk reads data from a chunk
func (cs *storageManager) ReadChunk(username *common.Username, offset, length, int64) ([]byte, error) {
	cs.chunkMutex.RLock()
	chunk, ok := cs.chunks[username]
	cs.chunkMutex.RUnlock()

	if !ok {
		return nil, common.ErrChunkNotFound
	}

	// validate offset and length
	if offset < 0 || offset >= common.ChunkSize {
		return nil, common.ErrInvalidOffset
	}

	// cap length to end of chunk
	if offset+length > common.ChunkSize {
		length = common.ChunkSize - offset
	}

	// Read from storage
	data, err := cs.storageManager.ReadChunk(username, offset, length)
	if err != nil {
		return nil, err
	}
	// verify checksum
	checksum := common.Checksum(data)
	if checksum != chunk.Checksum {
		return nil, common.ErrChecksumMismatch
	}
	return data, nil
}

// WriteChunk writes data to a chunk
func (cs *ChunkServer) WriteChunk(username common.ChunkUsername, offset uint64, data []byte) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return common.ErrChunkNotFound
	}

	// validate offset
	if offset < 0 || offset >= common.ChunkSize {
		return common.ErrInvalidOffset
	}

	// validate length
	if offset+uint64(len(data)) > common.ChunkSize {
		return common.ErrInvalidArgument
	}

	// write to storage
	err := cs.storageManager.WriteChunk(username, offset, data)
	if err != nil {
		return err
	}

	// Update chunk size if necessary
	newSize := offset + uint64(len(data))
	if newSize > chunk.Size {
		cs.chunkMutex.Lock()
		chunk.Size = newSize
		cs.chunkMutex.Unlock()

		// update metadata
		err = cs.storageManager.UpdateMetadata(username, chunk.Version, newSize)
		if err != nil {
			log.Printf("Failed to update metadata: %v", err)
		}
	}
	return nil
}

// ApplyMutation applies a mutation to a chunk
func (cs *ChunkServer) ApplyMutation(username common.ChunkUsername, mutation common.Mutation) error {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()

	// add to mutation queue
	cs.mutations[username] = append(cs.mutations[username], mutation)
	return nil
}

// AppendChunk appends data to a chunk
func (cs *ChunkServer) AppendChunk(username common.ChunkUsername, data []byte) (int64, error) {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return 0, common.ErrChunkNotFound
	}

	// Check if there's enough space
	if chunk.Size+uint64(len(data)) > common.ChunkSize {
		return 0, common.ErrInvalidOffset
	}

	// Append to storage
	err := cs.storageManager.WriteChunk(username, chunk.Size, data)
	if err != nil {
		return 0, err
	}

	// Update chunk size
	chunk.Size += uint64(len(data))

	// Update metadata
	err = cs.storageManager.UpdateMetadata(username, chunk.Version, chunk.Size)
	if err != nil {
		log.Printf("Failed to update metadata: %v", err)
	}

	return offset, nil
}

// SendHeartbeats sends heartbeats to the master
func (cs *ChunkServer) SendHeartbeats() {
	ticket := time.NewTicker(time.Duration(cs.config.HeartbeatInterval) * time.Second)
	defer ticket.Stop()

	// create rpc client for master
	masterClient, err := rpc.NewClient(cs.config.MasterAddress)
	if err != nil {
		log.Printf("Failed to create rpc client: %v", err)
		return
	}
	defer func() {
		if masterClient != nil {
			masterClient.Close()
		}
	}()

	for {
		select {
		case <-ticket.C:
			// if no valid client, create a new one
			if masterClient == nil{
				masterClient, err = rpc.NewClient(cs.config.MasterAddress)
				if err != nil {
					log.Printf("Failed to create rpc client: %v", err)
					continue
				}
			}

			// Get list of chunks
			cs.chunkMutex.RLock()
			handles := make([]common.ChunkUsername, 0, len(cs.chunks))
			for handle := range cs.chunks {
				handles = append(handles, handle)
			}
			cs.chunkMutex.RUnlock()

			// Get storage stats
			capacity, used, err := cs.storageManager.GetStats()

			// heartbeat request
			args := &common.HeartbeatRequest{
				Address: cs.config.Address,
				Chunks: handles,
				Capacity: capacity,
				UsedSpace: used,
			}

			// Prepare reply struct
			reply := &rpc.HeartbeatReply{}

			// make rpc call with timeout(ofcourse)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// create a channel to receive the response
			done := make(chan error, 1)
			go func() {
				// call the Heartbeat method on the master
				err := masterClient.Call(ctx, "Master.HandleHeartbeat", args, reply)
				done <- err
			}()

			// wait for the response or timeout
			select {
			case err := <-done:
				if err != nil {
					log.Printf("Failed to send heartbeat: %v", err)
					// close client, will recreate on the next heartbeat
					masterClient.Close()
					masterClient = nil
				} else if reply.Status != common.StatusOK {
					log.Printf("Heartbeat failed: %v", reply.Status)
				} else {
					log.Printf("Heartbeat successful: reported %d chunks, %d/%d storage used", len(handles), used, capacity)

					// Process reply from master
					if len(reply.ChunksToDelete) > 0 {
						log.Printf("Deleting %d chunks", len(reply.ChunksToDelete))
						go cs.deleteChunks(reply.ChunksToDelete)
					}

					if len(reply.ChunksToReplicate) > 0 {
						log.Printf("Replicating %d chunks", len(reply.ChunksToReplicate))
						go cs.replicateChunks(reply.ChunksToReplicate)
					}
				}
			case <-ctx.Done():
				log.Printf("Heartbeat timed out after 5 seconds")
				// close client, will recreate on the next heartbeat
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

// ProcessPendingMutations processes pending mutations
func (cs *ChunkServer) ProcessPendingMutations() {
	cs.mutationMutex.Lock()
	defer cs.mutationMutex.Unlock()

	// pick mutation from the queue
	for username, mutations := range cs.mutations {
		if len(mutation) == 0 {
			continue
		}

		// process mutations in order
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

		// clear processed mutations
		delete(cs.mutations, username)
	}
}
