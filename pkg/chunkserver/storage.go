package chunkserver

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

type StorageManager struct {
	root  string
	mutex sync.RWMutex
}

// NewStorageManager creates a new storage manager
func NewStorageManager(root string) *StorageManager {
	if err := os.MkdirAll(root, 0755); err != nil {
		log.Fatalf("Failed to create root directory: %v", err)
	}
	return &StorageManager{root: root}
}

// LoadChunks loads chunk usernames from disk
func (sm *StorageManager) LoadChunks() ([]common.ChunkUsername, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	var usernames []common.ChunkUsername

	err := filepath.Walk(sm.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".chunk" {
			filename := filepath.Base(path)
			var username common.ChunkUsername
			if _, err := fmt.Sscanf(filename, "%d.chunk", &username); err != nil {
				return nil // skip invalid files
			}
			usernames = append(usernames, username)
		}
		return nil
	})

	return usernames, err
}

// CreateChunk creates a new chunk file and its metadata file
func (sm *StorageManager) CreateChunk(username common.ChunkUsername) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	path := sm.getChunkPath(username)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	f.Close()

	metaPath := sm.getMetadataPath(username)
	metaFile, err := os.Create(metaPath)
	if err != nil {
		os.Remove(path)
		return err
	}
	defer metaFile.Close()

	// initialize metadata: size(8) | version(8) | checksum(4)
	var buf [20]byte
	_, err = metaFile.Write(buf[:])
	return err
}

// readChunk reads length bytes starting at offset from a chunk file
func (sm *StorageManager) readChunk(username common.ChunkUsername, offset uint64, length uint64) ([]byte, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	f, err := os.Open(sm.getChunkPath(username))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err = f.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	n, err := f.Read(data)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return data[:n], nil
}

// WriteChunk writes data at offset in a chunk file
func (sm *StorageManager) WriteChunk(username common.ChunkUsername, offset uint64, data []byte) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	f, err := os.OpenFile(sm.getChunkPath(username), os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}

	_, err = f.Write(data)
	return err
}

// DeleteChunk removes the chunk file and its metadata
func (sm *StorageManager) DeleteChunk(username common.ChunkUsername) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if err := os.Remove(sm.getChunkPath(username)); err != nil {
		return err
	}
	metaPath := sm.getMetadataPath(username)
	if err := os.Remove(metaPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// ReadMetadata reads the version and size from the metadata file
func (sm *StorageManager) ReadMetadata(username common.ChunkUsername) (common.ChunkVersion, uint64, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	f, err := os.Open(sm.getMetadataPath(username))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var buf [20]byte
	if _, err = io.ReadFull(f, buf[:]); err != nil {
		return 0, 0, err
	}

	size := binary.LittleEndian.Uint64(buf[:8])
	version := common.ChunkVersion(binary.LittleEndian.Uint64(buf[8:16]))
	return version, size, nil
}

// UpdateMetadata persists version and size to the metadata file
func (sm *StorageManager) UpdateMetadata(username common.ChunkUsername, version common.ChunkVersion, size uint64) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	f, err := os.OpenFile(sm.getMetadataPath(username), os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [20]byte
	binary.LittleEndian.PutUint64(buf[:8], size)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(version))
	binary.LittleEndian.PutUint32(buf[16:20], crc32.ChecksumIEEE(buf[:16]))

	_, err = f.WriteAt(buf[:], 0)
	return err
}

// GetStats returns total capacity and used bytes for the storage root
func (sm *StorageManager) GetStats() (uint64, uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(sm.root, &stat); err != nil {
		return 0, 0, err
	}

	capacity := stat.Blocks * uint64(stat.Bsize)
	available := stat.Bavail * uint64(stat.Bsize)
	used := capacity - available
	return capacity, used, nil
}

// GetBlockChecksum returns the CRC32 checksum of a 64 KB block within a chunk
func (sm *StorageManager) GetBlockChecksum(username common.ChunkUsername, blockIndex uint64) (uint32, error) {
	const blockSize = uint64(64 * 1024)
	data, err := sm.readChunk(username, blockIndex*blockSize, blockSize)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(data), nil
}

func (sm *StorageManager) getChunkPath(username common.ChunkUsername) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.chunk", username))
}

func (sm *StorageManager) getMetadataPath(username common.ChunkUsername) string {
	return filepath.Join(sm.root, fmt.Sprintf("%d.meta", username))
}
