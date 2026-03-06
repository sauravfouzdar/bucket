package chunkserver

import (
	"fmt"
	"log"
	netrpc "net/rpc"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// RPCCreateChunkArgs are the arguments for RPCCreateChunk
type RPCCreateChunkArgs struct {
	Username common.ChunkUsername
	Version  common.ChunkVersion
}

// RPCCreateChunkReply is the reply from RPCCreateChunk
type RPCCreateChunkReply struct {
	Status common.Status
}

// RPCCreateChunk creates a new chunk via RPC
func (cs *ChunkServer) RPCCreateChunk(args *RPCCreateChunkArgs, reply *RPCCreateChunkReply) error {
	if err := cs.CreateChunk(args.Username, args.Version); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// RPCReadChunkArgs are the arguments for RPCReadChunk
type RPCReadChunkArgs struct {
	Username common.ChunkUsername
	Offset   uint64
	Length   uint64
}

// RPCReadChunkReply is the reply from RPCReadChunk
type RPCReadChunkReply struct {
	Data   []byte
	Status common.Status
}

// RPCReadChunk reads data from a chunk via RPC
func (cs *ChunkServer) RPCReadChunk(args *RPCReadChunkArgs, reply *RPCReadChunkReply) error {
	data, err := cs.ReadChunk(args.Username, args.Offset, args.Length)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Data = data
	reply.Status = common.StatusOK
	return nil
}

// RPCWriteChunkArgs are the arguments for RPCWriteChunk
type RPCWriteChunkArgs struct {
	Username common.ChunkUsername
	Offset   uint64
	Data     []byte
}

// RPCWriteChunkReply is the reply from RPCWriteChunk
type RPCWriteChunkReply struct {
	Status common.Status
}

// RPCWriteChunk writes data to a chunk via RPC (legacy, no lease enforcement)
func (cs *ChunkServer) RPCWriteChunk(args *RPCWriteChunkArgs, reply *RPCWriteChunkReply) error {
	if err := cs.WriteChunk(args.Username, args.Offset, args.Data); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// --- Lease-based write protocol RPCs ---

// RPCGrantVersion is called by the master to bump a chunk's version before granting a lease
func (cs *ChunkServer) RPCGrantVersion(args *common.GrantVersionArgs, reply *common.GrantVersionReply) error {
	cs.chunkMutex.Lock()
	defer cs.chunkMutex.Unlock()

	chunk, ok := cs.chunks[args.Username]
	if !ok {
		reply.Status = common.StatusNotFound
		return nil
	}

	if args.NewVersion <= chunk.Version {
		reply.Status = common.StatusError
		return nil
	}

	chunk.Version = args.NewVersion

	if err := cs.storageManager.UpdateMetadata(args.Username, args.NewVersion, chunk.Size); err != nil {
		log.Printf("Failed to persist version update for chunk %d: %v", args.Username, err)
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}

// RPCPrimaryWriteArgs are the arguments for RPCPrimaryWrite
type RPCPrimaryWriteArgs struct {
	Username    common.ChunkUsername
	Version     common.ChunkVersion
	Offset      uint64
	Data        []byte
	Secondaries []string
}

// RPCPrimaryWriteReply is the reply from RPCPrimaryWrite
type RPCPrimaryWriteReply struct {
	Status common.Status
}

// RPCPrimaryWrite is called by the client on the primary chunkserver.
// It assigns a serial number, applies the mutation locally, and forwards to secondaries.
func (cs *ChunkServer) RPCPrimaryWrite(args *RPCPrimaryWriteArgs, reply *RPCPrimaryWriteReply) error {
	// 1. Verify version matches
	cs.chunkMutex.RLock()
	chunk, ok := cs.chunks[args.Username]
	cs.chunkMutex.RUnlock()

	if !ok {
		reply.Status = common.StatusNotFound
		return nil
	}
	if chunk.Version != args.Version {
		reply.Status = common.StatusError
		return nil
	}

	// 2. Assign serial number
	cs.writeIDMutex.Lock()
	cs.nextWriteID++
	writeID := cs.nextWriteID
	cs.writeIDMutex.Unlock()

	// 3. Apply mutation locally
	if err := cs.WriteChunk(args.Username, args.Offset, args.Data); err != nil {
		reply.Status = common.StatusError
		return nil
	}

	// 4. Forward to all secondaries
	var forwardErr error
	for _, addr := range args.Secondaries {
		conn, dialErr := netrpc.Dial("tcp", addr)
		if dialErr != nil {
			log.Printf("dial secondary %s: %v", addr, dialErr)
			forwardErr = dialErr
			continue
		}
		secArgs := &RPCSecondaryWriteArgs{
			Username: args.Username,
			Version:  args.Version,
			WriteID:  writeID,
			Offset:   args.Offset,
			Data:     args.Data,
		}
		secReply := &RPCSecondaryWriteReply{}
		callErr := conn.Call("ChunkServer.RPCSecondaryWrite", secArgs, secReply)
		conn.Close()
		if callErr != nil || secReply.Status != common.StatusOK {
			log.Printf("secondary write on %s failed: call=%v status=%v", addr, callErr, secReply.Status)
			forwardErr = fmt.Errorf("secondary %s failed", addr)
		}
	}

	// 5. Track that we are primary for this chunk
	cs.primaryMutex.Lock()
	cs.primaryChunks[args.Username] = true
	cs.primaryMutex.Unlock()

	if forwardErr != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// RPCSecondaryWriteArgs are the arguments for RPCSecondaryWrite
type RPCSecondaryWriteArgs struct {
	Username common.ChunkUsername
	Version  common.ChunkVersion
	WriteID  common.WriteID
	Offset   uint64
	Data     []byte
}

// RPCSecondaryWriteReply is the reply from RPCSecondaryWrite
type RPCSecondaryWriteReply struct {
	Status common.Status
}

// RPCSecondaryWrite is called by the primary to apply a mutation on a secondary replica
func (cs *ChunkServer) RPCSecondaryWrite(args *RPCSecondaryWriteArgs, reply *RPCSecondaryWriteReply) error {
	cs.chunkMutex.RLock()
	chunk, ok := cs.chunks[args.Username]
	cs.chunkMutex.RUnlock()

	if !ok {
		reply.Status = common.StatusNotFound
		return nil
	}
	if chunk.Version != args.Version {
		reply.Status = common.StatusError
		return nil
	}

	if err := cs.WriteChunk(args.Username, args.Offset, args.Data); err != nil {
		reply.Status = common.StatusError
		return nil
	}

	reply.Status = common.StatusOK
	return nil
}
