package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"log"


	"github.com/sauravfouzdar/pkg/common"
	"github.com/sauravfouzdar/pkg/chunkserver"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", "", "Chunk server address")
	masterAddr := flag.String("master", common.DefaultChunkServerConfig.MasterAddress, "Master address")
	storageRoot := flag.String("root", "/tmp/chunks", "Storage root directory")
	heartbeatInterval := flag.Int("heartbeat", common.DefaultChunkServerConfig.HeartbeatInterval, "Heartbeat interval in seconds")
	maxChunks := flag.Int("max-chunks", common.DefaultChunkServerConfig.MaxChunks, "Maximum number of chunks to store")
	flag.Parse()

	// Create chunk server configuration
	config := common.ChunkServerConfig{
		Address:          *addr,
		MasterAddress:    *masterAddr,
		StorageRoot:      *storageRoot,
		HeartbeatInterval: *heartbeatInterval,
		MaxChunks:        *maxChunks,
	}

	// Create and start chunk server
	cs, err := chunkserver.NewChunkServer(config)
	if err != nil {
		fmt.Printf("Failed to create chunk server: %v\n", err)
	}

	if err := cs.Start(); err != nil {
		fmt.Printf("Failed to start chunk server: %v\n", err)
	}

	// signal for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	// Shutdown chunk server
	cs.Shutdown()
	fmt.Println("Chunk server stopped")
}
