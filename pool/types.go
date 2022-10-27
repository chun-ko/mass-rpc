package pool

import (
	"net/rpc"
	"sync"
	"time"
)

type rpcClient interface {
	Close() error
	Go(serviceMethod string, args any, reply any, done chan *rpc.Call) *rpc.Call
}

// rpcConn is a wrapper for a single rpc client
type rpcConn struct {
	id            string       // A unique id to identify a connection
	pool          *RpcConnPool // The TCP connecion pool
	Conn          rpcClient    // The underlying TCP connection
	idleTimer     *time.Timer
	stopTimerChan chan bool
}

// connRequest wraps a channel to receive a connection
// and a channel to receive an error
type connRequest struct {
	connChan chan *rpcConn
	errChan  chan error
}

// RpcConnPool represents a pool of tcp connections
type RpcConnPool struct {
	addr         string
	mu           sync.Mutex          // mutex to prevent race conditions
	idleConns    map[string]*rpcConn // holds the idle connections
	numOpen      int                 // counter that tracks open connections
	maxOpenCount int
	maxIdleCount int
	// A queue of connection requests
	requestChan chan *connRequest
	//Maximum Idle time in seconds
	maxIdleTime          int
	closed               bool
	isTls                bool
	acceptSelfSignedCert bool
}

type RpcConnPoolConfig struct {
	MaxOpenCount int
	MaxIdleCount int
	MaxIdleTime  int
}
