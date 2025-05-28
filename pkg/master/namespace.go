package master


func NewNamespace() *Namespace {
	return &Namespace{
		Name:    "/",
		IsDirectory: false,
		FileID: common.FileID,
		//Children: make(map[string]*NamespaceNode),
	}
}

// LookupFile looks up a file in the namespace
func (ns *Namespace) LookupFile(path string) (common.FileID, error) {
}

// CreateFile creates a new file in the namespace
func (ns *Namespace) CreateFile(path string, fileID common.FileID) error {
}

// DeleteFile deletes a file from the namespace
func (ns *Namespace) DeleteFile(path string) error {
}

// ListDirectory lists the contents of a directory
func (ns *Namespace) ListDirectory(path string) ([]string, error) {
}

// CreateDirectory creates a new directory
func (ns *Namespace) CreateDirectory(path string) error {
}

// DeleteDirectory deletes a directory
func (ns *Namespace) DeleteDirectory(path string) error {
}

