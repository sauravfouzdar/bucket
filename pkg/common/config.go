package common

// Configuration for the bucket components
type MasterConfig struct {
	Address            string
	HeartbeatInterval  int // seconds
	ChunkReplicaNum    int // default no. of replicas for a chunk - 3
	LeaseTimeout       int // seconds
	CheckpointInterval int // seconds
}

// Configuration for the chunkserver
type ChunkServerConfig struct {
	Address           string
	MasterAddress     string
	StorageRoot       string
	HeartbeatInterval int // seconds
	MaxChunks         int // max no. of chunks a chunkserver can store
	CacheTimeout      int // seconds
}

type ClientConfig struct {
	MasterAddress string
	CacheTimeout  int // seconds
}

// Default configurations
var (
	DefaultMasterConfig = MasterConfig{
		Address:            "localhost:8000",
		HeartbeatInterval:  5,
		ChunkReplicaNum:    3,
		LeaseTimeout:       60,
		CheckpointInterval: 300,
	}

	DefaultChunkServerConfig = ChunkServerConfig{
		Address:           "localhost:8001",
		MasterAddress:     "localhost:8000",
		StorageRoot:       "/tmp/chunks",
		HeartbeatInterval: 5,
		MaxChunks:         100,
		CacheTimeout:      300,
	}

	DefaultClientConfig = ClientConfig{
		MasterAddress: "localhost:8000",
		CacheTimeout:  300,
	}
)
