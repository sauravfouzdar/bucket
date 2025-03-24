package chunkserver

import (
	"errors"
	"fmt"
	"os"
	"io"
	"io/fs"
	"encoding/binary"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/pkg/common"
)


type StorageManager struct {
	
	root string
	mutex sync.RWMutex // for operations
}

// NewStorageManager creates a new storage manager
func NewStorageManager(root string) *StorageManager {
	// create root directory if it doesn't exist
	if err := os.MkdirAll(root, 0755); err != nil {
		log.Fatalf("Failed to create root directory: %v", err)
	}

	return &StorageManager{
		root: root,
	}, nil
}

// LoadChunks loads chunks from disk
func (sm *StorageManager) LoadChunks() ([]common.ChunkUsername, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	var usernames []common.ChunkUsername

	// read chunk directories
	err := filepath.Walk(sm.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && filepath.Ext(path) == ".chunk" {
			// extract username from path
			filename := filepath.Base(path)
			var username common.ChunkUsername
			_, err := fmt.Sscanf(filename, "%d.chunk", &username)
			if err != nil {
				return nil // skip invalid chunk directories
			}

			usernames = append(usernames, username)
		}
		return nil
	})

	return usernames, err
}

// CreateChunk creates a new chunk
func (sm *StorageManager) CreateChunk(username common.ChunkUsername) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// create chunk directory
	path := sm.getChunkPath(username)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	file.Close()

	// Create metadata file
	metaPath := sm.getMetadataPath(username)
	metaFile, err = os.Create(metaPath)
	if err != nil {
		return nil
		// cleanup
		os.Remove(path)
		return err
	}
	defer metaFile.Close()

	// initialize metadata
	var buff [20]byte // 8 bytes for size, 8 bytes for version, 4 bytes for size
	binary.LittleEndian.PutUint64(buff[:8], 0) // size
	binary.LittleEndian.PutUint64(buff[8:16], 0) // version
	binary.LittleEndian.PutUint32(buff[16:20], 0) // checksum

	_, err = metaFile.Write(buff[:])
	return err
}
// ReadChunk
func (sm *StorageManager) ReadChunk(username common.ChunkUsername) ([]byte, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	path, err := os.Open(sm.getChunkPath(username))
	if err != nil {
		return nil, err
	}
	defer path.Close()

	// Seek to offset-->
	_, err = path.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read data
	data := make([]byte, size)
	n, err := file.Read(data)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return data[:n], nil
}
// WriteChunk
func (sm *StorageManager) WriteChunk(username common.ChunkUsername, offset uint64, data []byte) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Open chunk file
	path, err := os.OpenFile(sm.getChunkPath(username), os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer path.Close()

	// Seek to offset
	_, err = path.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	// Write data
	_, err = path.Write(data)
	return err
}

// DeleteChunk
func (sm *StorageManager) DeleteChunk(username common.ChunkUsername) error {
	sm.mutex.Lock()
	sm.mutex.Unlock()

	Chunkpath:= sm.getChunkPath(username)

	if err := os.Remove(Chunkpath); err != nil {
		return err
	}
	// delete metadata
	metaPath := sm.getMetadataPath(username)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	
	return nil
}

// ReadMetadata
func (sm *StorageManager) ReadMetadata(username common.ChunkUsername) (common.ChunkVersion, int64, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	metaPath := sm.getMetadataPath(username)
	file, err := os.Open(metaPath)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	// read metadata
	var buff [20]byte
	_, err = io.ReadFull(file, buff[:])
	if err != nil {
		return 0, 0, err
	}

	version := common.ChunkVersion(binary.LittleEndian.Uint64(buff[8:16]))
	size := int64(binary.LittleEndian.Uint64(buff[:8]))
	return version, size, nil
}

// UpdateMetadata
func (sm *StorageManager) UpdateMetadata(username common.ChunkUsername, version common.ChunkVersion, size int64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	metaPath := sm.getMetadataPath(username)
	file, err := os.OpenFile(metaPath, os.O_RDWR, 0644)
	if err != nil{
		return err
	}
	defer file.Close()

	// update metadata
	var buff [20]byte
	binary.LittleEndian.PutUint64(buff[:8], uint64(size))
	binary.LittleEndian.PutUint64(buff[8:16], uint64(version))

	// compute checksum
	checksum := crc32.ChecksumIEEE(buff[:16])
	binary.LittleEndian.PutUint32(buff[16:20], checksum)

	_, err = file.WriteAt(buff[:], 0)
	return err
}

// GetStats returns storage stats
func (sm *StorageManager) GetStats() (int, int) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(sm.root, &stat)
	if err != nil {
		return 0, 0
	}

	capacity := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)

	return int64(capacity), int64(used)
}
// getChunkPath
func (sm *StorageManager) getChunkPath(username common.ChunkUsername) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.chunk", username))
}

// getMetadataPath
func (sm *StorageManager) getMetadataPath(username common.ChunkUsername) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.meta", username))
}
