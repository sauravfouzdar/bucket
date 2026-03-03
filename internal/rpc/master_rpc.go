package rpc

import (
	"log"

	"github.com/sauravfouzdar/bucket/pkg/common"
	"github.com/sauravfouzdar/bucket/pkg/master"
)

// MasterService define rpc methods for master
type MasterService struct {
	master *master.Master
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

	fileID, err := ms.master.CreateFile(args.Path)
	if err != nil {
		reply.Status = common.StatusError
		return nil
	}
	reply.FileID = fileID
	reply.Status = common.StatusOK
	return nil
}

// tbd

