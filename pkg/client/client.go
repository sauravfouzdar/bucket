package client

import (
	"fmt"
	netrpc "net/rpc"
	"sync"
	"time"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

// Config holds client configuration
type Config struct {
	MasterAddress           string
	MetadataCacheExpiration time.Duration
	LocationCacheExpiration time.Duration
}

// Client communicates with the master and chunk servers
type Client struct {
	MasterAddress string
	metadataCache *MetadataCache
	locationCache *LocationCache
	config        Config
	masterConn    *netrpc.Client
	masterMu      sync.Mutex
}

// File represents an open file handle
type File struct {
	client   *Client
	path     string
	fileInfo *common.FileMetadata
	handles  []common.ChunkHandle
	mu       sync.Mutex
}

// NewClient creates a new client
func NewClient(config Config) (*Client, error) {
	if config.MetadataCacheExpiration == 0 {
		config.MetadataCacheExpiration = 60 * time.Second
	}
	if config.LocationCacheExpiration == 0 {
		config.LocationCacheExpiration = 60 * time.Second
	}

	c := &Client{
		MasterAddress: config.MasterAddress,
		config:        config,
		metadataCache: &MetadataCache{
			entries:    make(map[string]*CachedMetadata),
			expiration: config.MetadataCacheExpiration,
		},
		locationCache: &LocationCache{
			entries:    make(map[common.ChunkHandle]*CachedLocation),
			expiration: config.LocationCacheExpiration,
		},
	}
	return c, nil
}

// masterCall dials the master (reusing connection) and invokes method
func (c *Client) masterCall(method string, args, reply interface{}) error {
	c.masterMu.Lock()
	defer c.masterMu.Unlock()

	if c.masterConn == nil {
		var err error
		c.masterConn, err = netrpc.Dial("tcp", c.MasterAddress)
		if err != nil {
			return fmt.Errorf("connect to master: %w", err)
		}
	}

	err := c.masterConn.Call(method, args, reply)
	if err != nil {
		c.masterConn.Close()
		c.masterConn = nil
	}
	return err
}

// Create creates a new file
func (c *Client) Create(path string) (*File, error) {
	type args struct{ Path string }
	type reply struct {
		FileID common.FileID
		Status common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientCreateFile", &args{Path: path}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("create %q: status %d", path, r.Status)
	}
	return &File{client: c, path: path}, nil
}

// Open opens an existing file
func (c *Client) Open(path string) (*File, error) {
	type args struct{ Path string }
	type reply struct {
		Info   common.FileMetadata
		Status common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientGetFileInfo", &args{Path: path}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("open %q: file not found", path)
	}
	info := r.Info
	return &File{client: c, path: path, fileInfo: &info}, nil
}

// Delete deletes a file
func (c *Client) Delete(path string) error {
	type args struct{ Path string }
	type reply struct{ Status common.Status }
	r := &reply{}
	if err := c.masterCall("Master.ClientDeleteFile", &args{Path: path}, r); err != nil {
		return err
	}
	if r.Status != common.StatusOK {
		return fmt.Errorf("delete %q: status %d", path, r.Status)
	}
	return nil
}

// GetFileInfo returns metadata for a file
func (c *Client) GetFileInfo(path string) (*common.FileMetadata, error) {
	type args struct{ Path string }
	type reply struct {
		Info   common.FileMetadata
		Status common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientGetFileInfo", &args{Path: path}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, common.ErrFileNotFound
	}
	info := r.Info
	return &info, nil
}

// ListDirectory lists the entries in a directory
func (c *Client) ListDirectory(path string) ([]string, error) {
	type args struct{ Path string }
	type reply struct {
		Entries []string
		Status  common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientListDir", &args{Path: path}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("ls %q: not found", path)
	}
	return r.Entries, nil
}

// CreateDirectory creates a directory
func (c *Client) CreateDirectory(path string) error {
	type args struct{ Path string }
	type reply struct{ Status common.Status }
	r := &reply{}
	if err := c.masterCall("Master.ClientCreateDir", &args{Path: path}, r); err != nil {
		return err
	}
	if r.Status != common.StatusOK {
		return fmt.Errorf("mkdir %q: status %d", path, r.Status)
	}
	return nil
}

// Close closes the file handle
func (f *File) Close() error {
	return nil
}

// Write writes data at the given offset, spanning chunks as needed.
// Uses the GFS lease protocol: asks master for primary, writes to primary,
// primary forwards to secondaries.
func (f *File) Write(offset int, data []byte) error {
	off := uint64(offset)
	remaining := data

	for len(remaining) > 0 {
		chunkIndex := int(off / common.ChunkSize)
		chunkOffset := off % common.ChunkSize

		// Ensure chunk exists (allocate if needed)
		alloc, err := f.client.allocateChunk(f.path, chunkIndex)
		if err != nil {
			return err
		}
		if len(alloc.Locations) == 0 {
			return fmt.Errorf("no chunkservers available for chunk %d", chunkIndex)
		}

		writeLen := uint64(len(remaining))
		if chunkOffset+writeLen > common.ChunkSize {
			writeLen = common.ChunkSize - chunkOffset
		}

		// Get primary lease and write via primary with retry
		const maxRetries = 3
		var writeErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			lease, leaseErr := f.client.getPrimaryLease(alloc.Handle)
			if leaseErr != nil {
				writeErr = leaseErr
				continue
			}
			err = f.client.writeToPrimary(
				lease.Primary,
				alloc.Handle,
				lease.Version,
				chunkOffset,
				remaining[:int(writeLen)],
				lease.Secondaries,
			)
			if err == nil {
				writeErr = nil
				break
			}
			writeErr = err
		}
		if writeErr != nil {
			return writeErr
		}

		remaining = remaining[int(writeLen):]
		off += writeLen
	}
	return nil
}

// Read reads length bytes starting at offset, spanning chunks as needed
func (f *File) Read(offset int, length int) ([]byte, error) {
	info, err := f.client.GetFileInfo(f.path)
	if err != nil {
		return nil, err
	}

	result := make([]byte, 0, length)
	remaining := length
	off := uint64(offset)

	for remaining > 0 {
		chunkIndex := int(off / common.ChunkSize)
		chunkOffset := off % common.ChunkSize

		if chunkIndex >= len(info.ChunkUsernames) {
			break
		}
		handle := info.ChunkUsernames[chunkIndex]

		locations, err := f.client.getChunkLocations(handle)
		if err != nil || len(locations) == 0 {
			return nil, fmt.Errorf("no locations for chunk %d", handle)
		}

		readLen := uint64(remaining)
		if chunkOffset+readLen > common.ChunkSize {
			readLen = common.ChunkSize - chunkOffset
		}

		data, err := f.client.readFromChunkServer(locations[0], handle, chunkOffset, readLen)
		if err != nil {
			return nil, err
		}

		result = append(result, data...)
		remaining -= len(data)
		off += uint64(len(data))

		if len(data) < int(readLen) {
			break // hit EOF on chunk
		}
	}
	return result, nil
}

// --- internal helpers ---

type leaseResult struct {
	Primary     string
	Secondaries []string
	Version     common.ChunkVersion
}

func (c *Client) getPrimaryLease(handle common.ChunkHandle) (*leaseResult, error) {
	type args struct {
		Handle common.ChunkHandle
	}
	type reply struct {
		Primary     string
		Secondaries []string
		Version     common.ChunkVersion
		Status      common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientGetPrimaryLease", &args{Handle: handle}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("get primary lease for chunk %d: status %d", handle, r.Status)
	}
	return &leaseResult{
		Primary:     r.Primary,
		Secondaries: r.Secondaries,
		Version:     r.Version,
	}, nil
}

func (c *Client) writeToPrimary(primaryAddr string, handle common.ChunkHandle, version common.ChunkVersion, offset uint64, data []byte, secondaries []string) error {
	conn, err := netrpc.Dial("tcp", primaryAddr)
	if err != nil {
		return fmt.Errorf("connect to primary %s: %w", primaryAddr, err)
	}
	defer conn.Close()

	type args struct {
		Username    common.ChunkUsername
		Version     common.ChunkVersion
		Offset      uint64
		Data        []byte
		Secondaries []string
	}
	type reply struct {
		Status common.Status
	}
	r := &reply{}
	if err := conn.Call("ChunkServer.RPCPrimaryWrite", &args{
		Username:    handle,
		Version:     version,
		Offset:      offset,
		Data:        data,
		Secondaries: secondaries,
	}, r); err != nil {
		return fmt.Errorf("primary write: %w", err)
	}
	if r.Status != common.StatusOK {
		return fmt.Errorf("primary write to chunk %d failed: status %d", handle, r.Status)
	}
	return nil
}

type allocResult struct {
	Handle    common.ChunkHandle
	Locations []string
	Version   common.ChunkVersion
}

func (c *Client) allocateChunk(path string, chunkIndex int) (*allocResult, error) {
	type args struct {
		Path       string
		ChunkIndex int
	}
	type reply struct {
		Handle    common.ChunkHandle
		Locations []string
		Version   common.ChunkVersion
		Status    common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientAllocateChunk", &args{Path: path, ChunkIndex: chunkIndex}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("allocate chunk %d of %q: status %d", chunkIndex, path, r.Status)
	}
	return &allocResult{Handle: r.Handle, Locations: r.Locations, Version: r.Version}, nil
}

func (c *Client) getChunkLocations(handle common.ChunkHandle) ([]string, error) {
	type args struct{ Handle common.ChunkHandle }
	type reply struct {
		Locations []string
		Status    common.Status
	}
	r := &reply{}
	if err := c.masterCall("Master.ClientGetChunkLocations", &args{Handle: handle}, r); err != nil {
		return nil, err
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("get locations for chunk %d: not found", handle)
	}
	return r.Locations, nil
}

func (c *Client) readFromChunkServer(addr string, handle common.ChunkHandle, offset, length uint64) ([]byte, error) {
	conn, err := netrpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect to chunkserver %s: %w", addr, err)
	}
	defer conn.Close()

	type args struct {
		Username common.ChunkUsername
		Offset   uint64
		Length   uint64
	}
	type reply struct {
		Data   []byte
		Status common.Status
	}
	r := &reply{}
	if err := conn.Call("ChunkServer.RPCReadChunk", &args{Username: handle, Offset: offset, Length: length}, r); err != nil {
		return nil, fmt.Errorf("read chunk: %w", err)
	}
	if r.Status != common.StatusOK {
		return nil, fmt.Errorf("read chunk %d: failed", handle)
	}
	return r.Data, nil
}

func (c *Client) writeToChunkServer(addr string, handle common.ChunkHandle, offset uint64, data []byte) error {
	conn, err := netrpc.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("connect to chunkserver %s: %w", addr, err)
	}
	defer conn.Close()

	type args struct {
		Username common.ChunkUsername
		Offset   uint64
		Data     []byte
	}
	type reply struct {
		Status common.Status
	}
	r := &reply{}
	if err := conn.Call("ChunkServer.RPCWriteChunk", &args{Username: handle, Offset: offset, Data: data}, r); err != nil {
		return fmt.Errorf("write chunk: %w", err)
	}
	if r.Status != common.StatusOK {
		return fmt.Errorf("write chunk %d: failed", handle)
	}
	return nil
}
