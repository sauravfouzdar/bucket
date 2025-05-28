package client

// Read reads data from a file
func (f *File) Read(offset, length int) ([]byte, error)

// Write writes data to a file
func (f *File) Write(offset int, data []byte) error

// Append appends data to a file
func (f *File) Append(data []byte) (int, error)

// Close closes a file
func (f *File) Close() error

// Flush flushes any cached data
func (f *File) Flush() error

// GetSize gets the size of a file
func (f *File) GetSize() (int64, error)

// getChunkForOffset gets the chunk index for a given offset
func (f *File) getChunkForOffset(offset int64) (int, error)

// readChunk reads data from a chunk
func (f *File) readChunk(chunkHandle common.ChunkHandle, offset, length int) ([]byte, error)

// writeChunk writes data to a chunk
func (f *File) writeChunk(chunkHandle common.ChunkHandle, offset int, data []byte) error

// appendChunk appends data to a chunk
func (f *File) appendChunk(chunkHandle common.ChunkHandle, data []byte) (int, error)
