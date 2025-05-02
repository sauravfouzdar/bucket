package client

import (
	"github.com/sauravfouzdar/bucket/pkg/common"
)	

type Client struct {
	MasterAddress string

	// Cache management
	metadataCache *MetadataCache
	locationCache *LocationCache

	// Configuration
	config Config
}

// File represents an open file handle
type File struct {
	client *Client
	path string
	fileInfo *common.FileMetadata
	chunkHandles []common.ChunkHandle
	mu sync.Mutex
}

// MetadataCache caches file metadata
type MetadataCache struct {
	entries map[string]*CachedMetadata
	mu sync.RWMutex
	expiration time.Duration
}

// CachedMetadata represents cached file metadata
type CachedMetadata struct {
	Metadata common.FileMetadata
	ChunkHandles []common.ChunkHandle
	ExpirationTime time.Time
}

// LocationCache caches chunk locations
type LocationCache struct {
	entries map[common.ChunkHandle]*CachedLocation
	mu sync.RWMutex
	expiration time.Duration
}

// CachedLocation represents cached chunk location
type CachedLocation struct {
	Locations []common.ChunkLocation
	ExpirationTime time.Time
}

// NewClient creates a new client
func NewClient(masterAddress string, config Config) (*Client, error) {
	client := &Client{
		MasterAddress: masterAddress,
		config: config,
	}

	client.metadataCache = &MetadataCache{
		entries: make(map[string]*CachedMetadata),
		expiration: config.MetadataCacheExpiration,
	}

	client.locationCache = &LocationCache{
		entries: make(map[common.ChunkHandle]*CachedLocation),
		expiration: config.LocationCacheExpiration,
	}

	return client, nil
}

// Create creates a new file
func (c *Client) Create(path string) (*File, error) {

}

// Open opens a file
func (c *Client) Open(path string) (*File, error) {
}

// Delete deletes a file
func (c *Client) Delete(path string) error {
}
// GetFileInfo gets information about a file
func (c *Client) GetFileInfo(path string) (*common.FileMetadata, error) {
}

// ListDirectory lists the contents of a directory
func (c *Client) ListDirectory(path string) ([]string, error) {
}