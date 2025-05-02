package rpc

import (
	"log"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// MasterService define rpc methods for master
type MasterService struct {
	master *Master
}

// CreateFileArgs defines arguments for CreateFile rpc
type CreateFileArgs struct {
	Path string
}

// CreateFileReply defines reply for CreateFile rpc
type CreateFileReply struct {
	FileID common.FileID
	Status common.Status
}

/// CreateFile creates a new file
func (ms *MasterService) CreateFile(args *CreateFileArgs, reply *CreateFileReply) error {
	log.Printf("CreateFile(%s)", args.Path)

	fileID, status := ms.master.CreateFile(args.Path)
	reply.FileID = fileID
	reply.Status = status

	return nil
}

// tbd

