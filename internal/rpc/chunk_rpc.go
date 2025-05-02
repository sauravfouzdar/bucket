package rpc

import (
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/chunkserver"
	"github.com/sauravfouzdar/bucket/pkg/common"
	//"github.com/sauravfouzdar/bucket/pkg/master"
)

// ChunkService defines RPC methods for the chunk server
type ChunkService struct {
	ChunkServer	*chunkserver.ChunkServer
	
	// for caching the data in append operation
	dataMutex	sync.RWMutex
	dataCache	map[string][]byte
	dataExpiry	map[string]time.Time
}

// NewChunkService creates a new chunk service
func NewChunkService(cs *chunkserver.ChunkServer) *ChunkService {
	service := ChunkService{
		ChunkServer: cs,
		dataCache: make(map[string][]byte),
		dataExpiry: map([string]time.Time),
	}

	return &service
}

// CleanupDataCache removes expired data from the cache periodically
func (s *ChunkService) CleanupDataCache() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.dataMutex.Lock()
			for key, expiry := range s.dataExpiry {
				if time.Now().After(expiry) {
					delete(s.dataCache, key)
					delete(s.dataExpiry, key)
				}
			}
			s.dataMutex.Unlock()
		}
	}
}

// ReadChunkArgs are the arguments to ReadChunk
type ReadChunkArgs struct {
	Username	common.ChunkUsername
	Offset		uint64
	Length		uint64
}

// ReadChunkReply is the reply from ReadChunk
type ReadChunkReply struct {
	Data	[]byte
	Status 	common.Status
}

// ReadChunk reads data from a chunk
func (s *ChunkService) ReadChunk(args *ReadChunkArgs, reply *ReadChunkReply) error {
	data, err := s.ChunkServer.ReadChunk(args.Username, args.Offset, args.Length)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Data = data
	reply.Status = common.StatusOK
	return nil
}

// WriteChunkArgs are the arguments to WriteChunk
type WriteChunkArgs struct {
	Username	common.ChunkUsername
	Offset		uint64
	Data		[]byte
}

type WriteChunkReply struct {
	Status	common.Status
}

// WriteChunk writes data to a chunk
func (s *ChunkService) WriteChunk(args *WriteChunkArgs, reply *WriteChunkReply) error {
	err := s.ChunkServer.WriteChunk(args.Username, args.Offset, args.Data)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}

// AppendChunkArgs are the arguments to AppendChunk
type AppendChunkArgs struct {
	Username	common.ChunkUsername
	Data		[]byte
}

// AppendChunkReply is the reply from AppendChunk
type AppendChunkReply struct {
	Status	common.Status
	Offset	uint64
}

// AppendChunk appends data to a chunk
func (s *ChunkService) AppendChunk(args *AppendChunkArgs, reply *AppendChunkReply) error {
	offset, err := s.ChunkServer.AppendChunk(args.Username, args.Data)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Offset = offset
	reply.Status = common.StatusOK
	return nil
}

// CreateChunkArgs are the arguments to CreateChunk
type CreateChunkArgs struct {
	Username	common.ChunkUsername
	Version		common.ChunkVersion
}

// CreateChunkReply is the reply from CreateChunk
type CreateChunkReply struct {
	Status	common.Status
}

// CreateChunk creates a new chunk
func (s *ChunkService) CreateChunk(args *CreateChunkArgs, reply *CreateChunkReply) error {
	err := s.ChunkServer.CreateChunk(args.Username, args.Version)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}

// DeleteChunkArgs are the arguments to DeleteChunk
type DeleteChunkArgs struct {
	Username	common.ChunkUsername
}

// DeleteChunkReply is the reply from DeleteChunk
type DeleteChunkReply struct {
	Status	common.Status
}

// DeleteChunk deletes a chunk
func (s *ChunkService) DeleteChunk(args *DeleteChunkArgs, reply *DeleteChunkReply) error {
	err := s.ChunkServer.DeleteChunk(args.Username)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}

// PushDataArgs are the arguments to PushData(used in write operation)
type PushDataArgs struct {
	Username	common.ChunkUsername
	Data		[]byte
}

// PushDataReply is the reply from PushData
type PushDataReply struct {
	Status	common.Status
	DataID	string
}

// PushData caches data for a write operation
func (s *ChunkService) PushData(args *PushDataArgs, reply *PushDataReply) error {
	// generate a unique data ID
	hash := md5.Sum(args.Data)
	dataID := hex.EncodeToString(hash[:]) + "-" + time.Now().Format("20060102150405")

	// cache data with 5 minute expiry
	s.dataMutex.Lock()
	s.dataCache[dataID] = args.Data
	s.dataExpiry[dataID] = time.Now().Add(5 * time.Minute)
	s.dataMutex.Unlock()

	reply.DataID = dataID
	reply.Status = common.StatusOK
	return nil
}

// ApplyMutationArgs are the arguments to ApplyMutation
type ApplyMutationArgs struct {
	Username	common.ChunkUsername
	Mutation	common.Mutation
	DataID		string
	Secondary	[]string // additional replicas to forward the mutation to
}

// ApplyMutationReply is the reply from ApplyMutation
type ApplyMutationReply struct {
	Status	common.Status
}

// ApplyMutation applies a mutation to a chunk
func (s *ChunkService) ApplyMutation(args *ApplyMutationArgs, reply *ApplyMutationReply) error {
	// retrieve data from cache
	s.dataMutex.RLock()
	data, ok := s.dataCache[args.DataID]
	s.dataMutex.RUnlock()

	if !ok {
		reply.Status = common.StatusError
		return nil
	}

	// create a mutation (with data)
	mutation := args.Mutation
	mutation.Data = data

	// apply mutation locally
	err := s.ChunkServer.ApplyMutation(args.Username, mutation)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	// forward mutation to secondary replicas
	if len(args.Secondary) > 0 {
		forwardArgs := ForwardMutationArgs{
			Username: args.Username,
			Mutation: mutation,
			DataID:   args.DataID,
		}

		// forward mutation to secondary replicas in parallel
		var wg sync.WaitGroup
		errors := make(chan error, len(args.Secondary))
		for _, server := range args.Secondary {
			wg.Add(1)
			go func(server string) {
				defer wg.Done()

				client, err := rpc.NewClient(server)
				if err != nil {
					errors <- err
					return
				}
				defer client.Close()

				forwardReply := &ForwardMutationReply{}
				err = client.CallWithTimeout("ChunkService.ForwardMutation", &forwardArgs, forwardReply, 5*time.Second)
				if err != nil {
					errors <- err
					return
				}

				if forwardReply.Status != common.StatusOK {
					errors <- fmt.Errorf("forward mutation failed: %v", forwardReply.Status)
					return
				}
			}(server)
		}

		// wait for all forward operations to complete
		wg.Wait()
		close(errors)

		// check for errors
		for err := range errors {
			if err != nil {
				log.Printf("Failed to forward mutation: %v", err)
				reply.Status = common.StatusError
				return nil
			}
		}
	}

	reply.Status = common.StatusOK
	return nil
}

// ForwardMutationArgs are the arguments to ForwardMutation
type ForwardMutationArgs struct {
	Username	common.ChunkUsername
	Mutation	common.Mutation
	DataID		string
}

// ForwardMutationReply is the reply from ForwardMutation
type ForwardMutationReply struct {
	Status	common.Status
}

// ForwardMutation forwards a mutation to a secondary replica
func (s *ChunkService) ForwardMutation(args *ForwardMutationArgs, reply *ForwardMutationReply) error {
	// apply mutation locally
	err := s.ChunkServer.ApplyMutation(args.Username, args.Mutation)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}

// SendCopyChunkArgs are the arguments for sending a chunk for replication
type SendCopyChunkArgs struct {
	Username	common.ChunkUsername
}

// SendCopyChunkReply is the reply from SendCopyChunk
type SendCopyChunkReply struct {
	Status		common.Status
	Version		common.ChunkVersion
	Data 		[]byte
}

// SendCopyChunk sends a chunk for replication
func (s *ChunkService) SendCopyChunk(args *SendCopyChunkArgs, reply *SendCopyChunkReply) error {
	// get chunk info
	chunk, err := s.ChunkServer.GetChunkInfo(args.Username)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	// read chunk data
	data, err := s.ChunkServer.ReadChunk(args.Username, 0, chunk.Size)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Version = chunk.Version
	reply.Data = data
	reply.Status = common.StatusOK
	return nil
}

// GetChecksumArgs are the arguments to GetChecksum
type GetChecksumArgs struct {
	Username	common.ChunkUsername
	BlockIndex	uint64
}

// GetChecksumReply is the reply from GetChecksum
type GetChecksumReply struct {
	Checksum	uint32
	Status		common.Status
}

// GetChecksum returns the checksum of a block
func (s *ChunkService) GetChecksum(args *GetChecksumArgs, reply *GetChecksumReply) error {
	var checksum uint32
	var err error

	if args.BlockIndex >= 0 {
		checksum, err := s.ChunkServer.GetBlockChecksum(args.Usernames, args.BlockIndex)
	} else {
		// get checksums for entire chunk
		checksum, err = s.ChunkServer.GetChunkChecksum(args.Usernames)
	}

	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Checksum = checksum
	reply.Status = common.StatusOK
	return nil
}

// GetChunkInfo returns metadata for a chunk
func (cs *chunkserver.ChunkServer) GetChunkInfo(username common.ChunkUsername) (*common.ChunkMetadata, error) {
	cs.chunkMutex.RLock()
	defer cs.chunkMutex.RUnlock()

	chunk, ok := cs.chunks[username]
	if !ok {
		return nil, common.ErrChunkNotFound
	}

	return &common.ChunkMetadata{
		Username: chunk.Username,
		Version: chunk.Version,
		Size: chunk.Size,
	}, nil
}

// GetBlockChecksum returns the checksum of a block in a chunk
func (cs *chunkserver.ChunkServer) GetBlockChecksum(username common.ChunkUsername, blockIndex uint64) (uint32, error) {
	return cs.storageManager.GetBlockChecksum(username, blockIndex)
}

// GetChunkChecksum returns the checksum of a chunk
func (cs *chunkserver.ChunkServer) GetChunkChecksum(username common.ChunkUsername) (uint32, error) {
	cs.chunkMutex.RLock()
	chunkk, ok := cs.chunks[username]
	cs.chunkMutex.RUnlock()

	if !ok {
		return 0, common.ErrChunkNotFound
	}

	// read entire chunk
	data, err := cs.storageManager.ReadChunk(username, 0, chunk.Size)
	if err != nil {
		return 0, err
	}

	// calculate checksum
	return common.Checksum(data), nil
}

// additional master methods used in RPC implementation
func (m *master.Master) GetChunkInfo(username common.ChunkUsername) (*common.ChunkMetadata, error) {
	return m.GetChunkInfo(username)
}

/// GetChunksToDelete returns a list of chunks to delete
func (m *master.Master) GetChunksToDelete(serverAddr string, reportedChunks []common.ChunkUsername) ([]common.ChunkUsername) {
	// get all chunks on the server
	allChunks := m.GetChunksOnServer(serverAddr)

	// find chunks to delete
	var chunksToDelete []common.ChunkUsername
	for _, chunk := range allChunks {
		found := false
		for _, reported := range reportedChunks {
			if chunk == reported {
				found = true
				break
			}
		}
		if !found {
			chunksToDelete = append(chunksToDelete, chunk)
		}
	}

	return chunksToDelete
}

type ReplicationTask struct {
	Username 		common.ChunkUsername
	SourceServer	string
	Version 		common.ChunkVersion
}

// GetChunksToReplicate returns a list of chunks to replicate
func (m *master.Master) GetChunksToReplicate(serverAddr string, reportedChunks []common.ChunkUsername) ([]ReplicationTask) {
	// implement later
	return []ReplicationTask{}
}

func (m *master.Master) AddChunkReplica(username common.ChunkUsername, serverAddr string)  {
	m.metadata.AddChunkLocation(username, serverAddr)
}

func (m *master.Master) MarkReplicationFailed(username common.ChunkUsername, serverAddr string) {
	// implement later
	log.Printf("Replication failed for chunk %s on server %s", username, serverAddr)
}