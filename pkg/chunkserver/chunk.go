package chunkserver

import (
	"sync"

	"github.com/sauravfouzdar/bucket/pkg/common"
)


type Chunk struct {
	Username		common.ChunkUsername
	Version			common.ChunkVersion
	Size			uint64
	mutex			sync.RWMutex
}

// NewChunk creates a new chunk
func NewChunk(username common.ChunkUsername, version common.ChunkVersion) *Chunk {
	return &Chunk{
		Username: username,
		Version: version,
		Size: 0,
	}
}




