package master

import (
	"log"
	netrpc "net/rpc"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// --- CreateFile ---

type CreateFileArgs struct {
	Path string
}

type CreateFileReply struct {
	FileID common.FileID
	Status common.Status
}

func (m *Master) ClientCreateFile(args *CreateFileArgs, reply *CreateFileReply) error {
	fileID, err := m.metadata.CreateFile(args.Path)
	if err != nil {
		if err == common.ErrFileExists {
			reply.Status = common.StatusAlreadyExists
		} else {
			reply.Status = common.StatusError
		}
		return nil
	}
	if nsErr := m.namespace.CreateFile(args.Path, fileID); nsErr != nil {
		log.Printf("namespace.CreateFile %s: %v", args.Path, nsErr)
	}
	reply.FileID = fileID
	reply.Status = common.StatusOK
	return nil
}

// --- GetFileInfo ---

type GetFileInfoArgs struct {
	Path string
}

type GetFileInfoReply struct {
	Info   common.FileMetadata
	Status common.Status
}

func (m *Master) ClientGetFileInfo(args *GetFileInfoArgs, reply *GetFileInfoReply) error {
	info, err := m.metadata.GetFileMetadata(args.Path)
	if err != nil {
		reply.Status = common.StatusNotFound
		return nil
	}
	reply.Info = *info
	reply.Status = common.StatusOK
	return nil
}

// --- DeleteFile ---

type DeleteFileArgs struct {
	Path string
}

type DeleteFileReply struct {
	Status common.Status
}

func (m *Master) ClientDeleteFile(args *DeleteFileArgs, reply *DeleteFileReply) error {
	if err := m.metadata.DeleteFile(args.Path); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	_ = m.namespace.DeleteFile(args.Path)
	reply.Status = common.StatusOK
	return nil
}

// --- AllocateChunk ---

type AllocateChunkArgs struct {
	Path       string
	ChunkIndex int
}

type AllocateChunkReply struct {
	Handle    common.ChunkHandle
	Locations []string
	Version   common.ChunkVersion
	Status    common.Status
}

func (m *Master) ClientAllocateChunk(args *AllocateChunkArgs, reply *AllocateChunkReply) error {
	info, err := m.metadata.GetFileMetadata(args.Path)
	if err != nil {
		reply.Status = common.StatusNotFound
		return nil
	}

	// Return existing chunk if it already has live locations
	if args.ChunkIndex < len(info.ChunkUsernames) {
		handle := info.ChunkUsernames[args.ChunkIndex]
		locations, locErr := m.metadata.GetChunkLocations(handle)
		if locErr == nil && len(locations) > 0 {
			chunkMeta, _ := m.metadata.GetChunkMetadata(handle)
			reply.Handle = handle
			reply.Locations = locations
			if chunkMeta != nil {
				reply.Version = chunkMeta.Version
			}
			reply.Status = common.StatusOK
			return nil
		}
	}

	// Allocate new chunk in metadata
	handle, err := m.metadata.CreateChunk(info.ID, common.ChunkIndex(args.ChunkIndex))
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}

	serverAddrs := m.pickChunkServers(m.config.ChunkReplicaNum)
	if len(serverAddrs) == 0 {
		reply.Status = common.StatusError
		return nil
	}

	version := common.ChunkVersion(1)
	var successAddrs []string

	type csCreateArgs struct {
		Username common.ChunkUsername
		Version  common.ChunkVersion
	}
	type csCreateReply struct {
		Status common.Status
	}

	for _, addr := range serverAddrs {
		conn, dialErr := netrpc.Dial("tcp", addr)
		if dialErr != nil {
			log.Printf("dial chunkserver %s: %v", addr, dialErr)
			continue
		}
		csArgs := &csCreateArgs{Username: handle, Version: version}
		csReply := &csCreateReply{}
		callErr := conn.Call("ChunkServer.RPCCreateChunk", csArgs, csReply)
		conn.Close()
		if callErr != nil || csReply.Status != common.StatusOK {
			log.Printf("CreateChunk on %s: %v", addr, callErr)
			continue
		}
		_ = m.metadata.AddChunkLocation(handle, addr)
		successAddrs = append(successAddrs, addr)
	}

	if len(successAddrs) == 0 {
		reply.Status = common.StatusError
		return nil
	}

	reply.Handle = handle
	reply.Locations = successAddrs
	reply.Version = version
	reply.Status = common.StatusOK
	return nil
}

// --- GetChunkLocations ---

type GetChunkLocationsArgs struct {
	Handle common.ChunkHandle
}

type GetChunkLocationsReply struct {
	Locations []string
	Status    common.Status
}

func (m *Master) ClientGetChunkLocations(args *GetChunkLocationsArgs, reply *GetChunkLocationsReply) error {
	locations, err := m.metadata.GetChunkLocations(args.Handle)
	if err != nil {
		reply.Status = common.StatusNotFound
		return nil
	}
	reply.Locations = locations
	reply.Status = common.StatusOK
	return nil
}

// --- CreateDirectory ---

type CreateDirArgs struct {
	Path string
}

type CreateDirReply struct {
	Status common.Status
}

func (m *Master) ClientCreateDir(args *CreateDirArgs, reply *CreateDirReply) error {
	if err := m.namespace.CreateDirectory(args.Path); err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.Status = common.StatusOK
	return nil
}

// --- ListDirectory ---

type ListDirArgs struct {
	Path string
}

type ListDirReply struct {
	Entries []string
	Status  common.Status
}

func (m *Master) ClientListDir(args *ListDirArgs, reply *ListDirReply) error {
	entries, err := m.namespace.ListDirectory(args.Path)
	if err != nil {
		reply.Status = common.StatusNotFound
		return nil
	}
	reply.Entries = entries
	reply.Status = common.StatusOK
	return nil
}
