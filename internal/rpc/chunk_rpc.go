package rpc

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	netrpc "net/rpc"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/chunkserver"
	"github.com/sauravfouzdar/bucket/pkg/common"
)

// ChunkService defines RPC methods for the chunk server
type ChunkService struct {
	ChunkServer *chunkserver.ChunkServer

	// for caching the data in append operation
	dataMutex sync.RWMutex
	dataCache map[string][]byte
	dataExpiry map[string]time.Time
}

// NewChunkService creates a new chunk service
func NewChunkService(cs *chunkserver.ChunkServer) *ChunkService {
	return &ChunkService{
		ChunkServer: cs,
		dataCache:  make(map[string][]byte),
		dataExpiry: make(map[string]time.Time),
	}
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
	Username common.ChunkUsername
	Offset   uint64
	Length   uint64
}

// ReadChunkReply is the reply from ReadChunk
type ReadChunkReply struct {
	Data   []byte
	Status common.Status
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
	Username common.ChunkUsername
	Offset   uint64
	Data     []byte
}

type WriteChunkReply struct {
	Status common.Status
}

// WriteChunk writes data to a chunk
func (s *ChunkService) WriteChunk(args *WriteChunkArgs, reply *WriteChunkReply) error {
	if err := s.ChunkServer.WriteChunk(args.Username, args.Offset, args.Data); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// AppendChunkArgs are the arguments to AppendChunk
type AppendChunkArgs struct {
	Username common.ChunkUsername
	Data     []byte
}

// AppendChunkReply is the reply from AppendChunk
type AppendChunkReply struct {
	Status common.Status
	Offset uint64
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
	Username common.ChunkUsername
	Version  common.ChunkVersion
}

// CreateChunkReply is the reply from CreateChunk
type CreateChunkReply struct {
	Status common.Status
}

// CreateChunk creates a new chunk
func (s *ChunkService) CreateChunk(args *CreateChunkArgs, reply *CreateChunkReply) error {
	if err := s.ChunkServer.CreateChunk(args.Username, args.Version); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// DeleteChunkArgs are the arguments to DeleteChunk
type DeleteChunkArgs struct {
	Username common.ChunkUsername
}

// DeleteChunkReply is the reply from DeleteChunk
type DeleteChunkReply struct {
	Status common.Status
}

// DeleteChunk deletes a chunk
func (s *ChunkService) DeleteChunk(args *DeleteChunkArgs, reply *DeleteChunkReply) error {
	if err := s.ChunkServer.DeleteChunk(args.Username); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// PushDataArgs are the arguments to PushData (used in write operation)
type PushDataArgs struct {
	Username common.ChunkUsername
	Data     []byte
}

// PushDataReply is the reply from PushData
type PushDataReply struct {
	Status common.Status
	DataID string
}

// PushData caches data for a write operation
func (s *ChunkService) PushData(args *PushDataArgs, reply *PushDataReply) error {
	hash := md5.Sum(args.Data)
	dataID := hex.EncodeToString(hash[:]) + "-" + time.Now().Format("20060102150405")

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
	Username  common.ChunkUsername
	Mutation  common.Mutation
	DataID    string
	Secondary []string // additional replicas to forward the mutation to
}

// ApplyMutationReply is the reply from ApplyMutation
type ApplyMutationReply struct {
	Status common.Status
}

// ApplyMutation applies a mutation to a chunk
func (s *ChunkService) ApplyMutation(args *ApplyMutationArgs, reply *ApplyMutationReply) error {
	s.dataMutex.RLock()
	data, ok := s.dataCache[args.DataID]
	s.dataMutex.RUnlock()

	if !ok {
		reply.Status = common.StatusError
		return nil
	}

	mutation := args.Mutation
	mutation.Data = data

	if err := s.ChunkServer.ApplyMutation(args.Username, mutation); err != nil {
		reply.Status = common.StatusError
		return nil
	}

	if len(args.Secondary) > 0 {
		forwardArgs := ForwardMutationArgs{
			Username: args.Username,
			Mutation: mutation,
			DataID:   args.DataID,
		}

		var wg sync.WaitGroup
		errs := make(chan error, len(args.Secondary))
		for _, server := range args.Secondary {
			wg.Add(1)
			go func(server string) {
				defer wg.Done()

				client, err := netrpc.Dial("tcp", server)
				if err != nil {
					errs <- err
					return
				}
				defer client.Close()

				forwardReply := &ForwardMutationReply{}
				if err = client.Call("ChunkService.ForwardMutation", &forwardArgs, forwardReply); err != nil {
					errs <- err
					return
				}
				if forwardReply.Status != common.StatusOK {
					errs <- fmt.Errorf("forward mutation failed: %v", forwardReply.Status)
				}
			}(server)
		}

		wg.Wait()
		close(errs)

		for err := range errs {
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
	Username common.ChunkUsername
	Mutation common.Mutation
	DataID   string
}

// ForwardMutationReply is the reply from ForwardMutation
type ForwardMutationReply struct {
	Status common.Status
}

// ForwardMutation applies a mutation on a secondary replica
func (s *ChunkService) ForwardMutation(args *ForwardMutationArgs, reply *ForwardMutationReply) error {
	if err := s.ChunkServer.ApplyMutation(args.Username, args.Mutation); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// SendCopyChunkArgs are the arguments for sending a chunk for replication
type SendCopyChunkArgs struct {
	Username common.ChunkUsername
}

// SendCopyChunkReply is the reply from SendCopyChunk
type SendCopyChunkReply struct {
	Status  common.Status
	Version common.ChunkVersion
	Data    []byte
}

// SendCopyChunk sends a chunk for replication
func (s *ChunkService) SendCopyChunk(args *SendCopyChunkArgs, reply *SendCopyChunkReply) error {
	chunk, err := s.ChunkServer.GetChunkInfo(args.Username)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

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
	Username   common.ChunkUsername
	BlockIndex uint64
	WholeChunk bool
}

// GetChecksumReply is the reply from GetChecksum
type GetChecksumReply struct {
	Checksum uint32
	Status   common.Status
}

// GetChecksum returns the checksum of a block or entire chunk
func (s *ChunkService) GetChecksum(args *GetChecksumArgs, reply *GetChecksumReply) error {
	var (
		checksum uint32
		err      error
	)

	if args.WholeChunk {
		checksum, err = s.ChunkServer.GetChunkChecksum(args.Username)
	} else {
		checksum, err = s.ChunkServer.GetBlockChecksum(args.Username, args.BlockIndex)
	}

	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Checksum = checksum
	reply.Status = common.StatusOK
	return nil
}
