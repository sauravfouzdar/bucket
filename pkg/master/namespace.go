package master

import (
	"strings"

	"github.com/sauravfouzdar/bucket/pkg/common"
)

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return []string{}
	}
	return strings.Split(trimmed, "/")
}

// NewNamespace creates a new namespace with a root directory node
func NewNamespace() *Namespace {
	return &Namespace{
		root: &NamespaceNode{
			Name:        "/",
			IsDirectory: true,
			Children:    make(map[string]*NamespaceNode),
		},
	}
}

// LookupFile returns the FileID for the given path
func (ns *Namespace) LookupFile(path string) (common.FileID, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return "", common.ErrFileNotFound
	}
	node := ns.root
	for _, part := range parts {
		child, ok := node.Children[part]
		if !ok {
			return "", common.ErrFileNotFound
		}
		node = child
	}
	if node.IsDirectory {
		return "", common.ErrFileNotFound
	}
	return node.FileID, nil
}

// CreateFile adds a file node to the namespace
func (ns *Namespace) CreateFile(path string, fileID common.FileID) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return common.ErrInvalidArgument
	}
	node := ns.root
	for _, part := range parts[:len(parts)-1] {
		child, ok := node.Children[part]
		if !ok {
			return common.ErrFileNotFound
		}
		if !child.IsDirectory {
			return common.ErrInvalidArgument
		}
		node = child
	}
	name := parts[len(parts)-1]
	if _, exists := node.Children[name]; exists {
		return common.ErrFileExists
	}
	node.Children[name] = &NamespaceNode{
		Name:     name,
		FileID:   fileID,
		Children: make(map[string]*NamespaceNode),
	}
	return nil
}

// DeleteFile removes a file node from the namespace
func (ns *Namespace) DeleteFile(path string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return common.ErrFileNotFound
	}
	node := ns.root
	for _, part := range parts[:len(parts)-1] {
		child, ok := node.Children[part]
		if !ok {
			return common.ErrFileNotFound
		}
		node = child
	}
	name := parts[len(parts)-1]
	entry, exists := node.Children[name]
	if !exists || entry.IsDirectory {
		return common.ErrFileNotFound
	}
	delete(node.Children, name)
	return nil
}

// ListDirectory lists the names of entries in a directory
func (ns *Namespace) ListDirectory(path string) ([]string, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	var node *NamespaceNode
	if path == "/" || path == "" {
		node = ns.root
	} else {
		parts := splitPath(path)
		node = ns.root
		for _, part := range parts {
			child, ok := node.Children[part]
			if !ok {
				return nil, common.ErrFileNotFound
			}
			node = child
		}
	}
	if !node.IsDirectory {
		return nil, common.ErrInvalidArgument
	}
	entries := make([]string, 0, len(node.Children))
	for name, child := range node.Children {
		if child.IsDirectory {
			entries = append(entries, name+"/")
		} else {
			entries = append(entries, name)
		}
	}
	return entries, nil
}

// CreateDirectory adds a directory node to the namespace
func (ns *Namespace) CreateDirectory(path string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return nil // root already exists
	}
	node := ns.root
	for _, part := range parts[:len(parts)-1] {
		child, ok := node.Children[part]
		if !ok {
			return common.ErrFileNotFound
		}
		if !child.IsDirectory {
			return common.ErrInvalidArgument
		}
		node = child
	}
	name := parts[len(parts)-1]
	if existing, exists := node.Children[name]; exists {
		if existing.IsDirectory {
			return nil // idempotent
		}
		return common.ErrFileExists
	}
	node.Children[name] = &NamespaceNode{
		Name:        name,
		IsDirectory: true,
		Children:    make(map[string]*NamespaceNode),
	}
	return nil
}

// DeleteDirectory removes a directory node from the namespace
func (ns *Namespace) DeleteDirectory(path string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parts := splitPath(path)
	if len(parts) == 0 {
		return common.ErrInvalidArgument
	}
	node := ns.root
	for _, part := range parts[:len(parts)-1] {
		child, ok := node.Children[part]
		if !ok {
			return common.ErrFileNotFound
		}
		node = child
	}
	name := parts[len(parts)-1]
	entry, exists := node.Children[name]
	if !exists || !entry.IsDirectory {
		return common.ErrFileNotFound
	}
	delete(node.Children, name)
	return nil
}
