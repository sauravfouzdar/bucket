package rpc

import (
	"log"
	"net"
	"net/rpc"
)

// Server in an RPC server
type Server struct {
	*rpc.Server
}

// NewServer creates a new RPC server
func NewServer() *Server {
	return &Server{
		Server: rpc.NewServer(),
	}
}

// Regsiter registers a rpc service with the server
func (s *Server) Register(rcvr interface{}) error {
	return s.Server.Register(rcvr)
}
