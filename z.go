package client

// NewClient creates a new client
func NewClient(masterAddress string, config Config) (*Client, error)

// Create creates a new file
func (c *Client) Create(path string) (*File, error)

// Open opens a file
func (c *Client) Open(path string) (*File, error)

// Delete deletes a file
func (c *Client) Delete(path string) error

// GetFileInfo gets information about a file
func (c *Client) GetFileInfo(path string) (*common.FileMetadata, error)

// ListDirectory lists the contents of a directory
func (c *Client) ListDirectory(path string) ([]string, error)