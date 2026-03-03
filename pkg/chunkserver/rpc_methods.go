package chunkserver

import "github.com/sauravfouzdar/bucket/pkg/common"

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

// RPCWriteChunk writes data to a chunk via RPC
func (cs *ChunkServer) RPCWriteChunk(args *RPCWriteChunkArgs, reply *RPCWriteChunkReply) error {
	if err := cs.WriteChunk(args.Username, args.Offset, args.Data); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}
