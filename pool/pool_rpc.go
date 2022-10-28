package pool

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

func (p *RpcConnPool) Put(c *rpcConn) error {

	if p.closed {
		p.mu.Lock()
		c.Conn.Close()
		p.numOpen--
		p.mu.Unlock()
		return errors.New("pool is closed")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.maxIdleCount > 0 && p.maxIdleCount > len(p.idleConns) {
		p.idleConns[c.id] = c
		c.idleTimer = time.NewTimer(time.Duration(p.maxIdleTime * int(time.Second)))
		go c.handleConnTimeout()
	} else {
		err := c.Conn.Close()
		if err != nil {
			log.Printf("Error in closing connection. Details: %s", err)
			return err
		}
		c.pool.numOpen--
	}
	return nil
}

func (p *RpcConnPool) Get() (*rpcConn, error) {
	if p.closed {
		return nil, errors.New("pool is closed")
	}
	p.mu.Lock()

	//Case 1: Pool has idle connections
	numIdle := len(p.idleConns)

	if numIdle > 0 {
		conn, err := p.reuseConnLocked()
		if err == nil {
			return conn, nil
		}
	}
	//Case 2: Pool does not have idle connections

	if p.maxOpenCount > 0 && p.numOpen >= p.maxOpenCount {

		// Create the request
		req := &connRequest{
			connChan: make(chan *rpcConn, 1),
			errChan:  make(chan error, 1),
		}

		p.mu.Unlock()

		p.requestChan <- req

		select {
		case rpcConn := <-req.connChan:
			return rpcConn, nil
		case err := <-req.errChan:
			return nil, err
		}
	}

	p.numOpen++
	p.mu.Unlock()
	fmt.Print("Create new rpc connection\n")
	newRpcConn, err := p.openNewRpcConn()

	if err != nil {
		log.Printf("Error in opening connection. Details: %s", err)
		p.mu.Lock()
		p.numOpen--
		p.mu.Unlock()
		return nil, err
	}

	return newRpcConn, nil
}

// This is called in the case a new connection is required and number of OpenConnections has still not reached its limit
func (p *RpcConnPool) openNewRpcConn() (*rpcConn, error) {
	if p.closed {
		return nil, errors.New("pool is Closed")
	}
	addr := p.addr

	var c *rpc.Client
	var err error

	if p.isTls {
		conf := &tls.Config{
			MinVersion: tls.VersionTLS13,
			CipherSuites: []uint16{
				tls.TLS_AES_256_GCM_SHA384,
				tls.TLS_CHACHA20_POLY1305_SHA256,
			},
		}

		if p.acceptSelfSignedCert { // Dev mode
			conf.InsecureSkipVerify = true
		}

		conn, err := net.Dial("tcp", addr)
		fmt.Printf("Connect to %v", addr)
		if err != nil {
			log.Print("Error while initiating connection before tls", err)
			return nil, err
		}
		tlsConn := tls.Client(conn, conf)
		c = rpc.NewClient(tlsConn)
	} else {
		fmt.Printf("Connect to %v", addr)
		c, err = rpc.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
	}
	conn := &rpcConn{
		id:            fmt.Sprintf("%v", time.Now().UnixNano()),
		pool:          p,
		Conn:          c,
		stopTimerChan: make(chan bool, 1),
	}
	return conn, nil
}

// Wait for connection until there is one returned by the request
func (p *RpcConnPool) handleConnectionRequest() {
	for req := range p.requestChan {
		var (
			reqDone        = false
			timeoutReached = false

			timeoutChan = time.After(10 * time.Second)
		)

		for {
			if reqDone || timeoutReached {
				break
			}

			select {
			case <-timeoutChan:
				timeoutReached = true
				req.errChan <- errors.New("connection request timeout")
			default:
				p.mu.Lock()
				//Case 1: Pool has idle connections
				numIdle := len(p.idleConns)
				if numIdle > 0 {
					conn, err := p.reuseConnLocked()
					if err == nil {
						req.connChan <- conn
						reqDone = true
					}
				} else if p.numOpen > 0 && p.maxOpenCount > p.numOpen { // Pool can open new connections
					p.numOpen++
					p.mu.Unlock()
					newRpcConn, err := p.openNewRpcConn()
					if err != nil {
						p.mu.Lock()
						p.numOpen--
						p.mu.Unlock()
					}
					req.connChan <- newRpcConn
					reqDone = true
				} else {
					p.mu.Unlock()
				}
			}
		}
	}
}

func (c *rpcConn) handleConnTimeout() {
	select {
	case <-c.idleTimer.C:
		c.pool.mu.Lock()
		c.Conn.Close()
		delete(c.pool.idleConns, c.id)
		c.pool.numOpen--
		c.pool.mu.Unlock()
	case <-c.stopTimerChan:
	}
}

func NewConnectionPool(
	addr string,
	maxOpenCount int,
	maxIdleCount int,
	maxIdleTime int,
	isTls bool,
	acceptSelfSignedCert bool,
) *RpcConnPool {
	pool := &RpcConnPool{
		addr:                 addr,
		idleConns:            make(map[string]*rpcConn),
		numOpen:              0,
		maxOpenCount:         maxOpenCount,
		maxIdleCount:         maxIdleCount,
		requestChan:          make(chan *connRequest),
		maxIdleTime:          maxIdleTime,
		closed:               false,
		isTls:                isTls,
		acceptSelfSignedCert: acceptSelfSignedCert,
	}
	go pool.handleConnectionRequest()

	return pool
}

func (p *RpcConnPool) Shutdown() error {
	p.mu.Lock()

	if p.closed {
		p.mu.Unlock()
		return nil
	}

	if p.requestChan != nil {
		close(p.requestChan)
	}

	for _, conn := range p.idleConns {
		conn.Conn.Close()
		ok := conn.idleTimer.Stop()
		if ok {
			conn.stopTimerChan <- true
		}
		p.numOpen--
	}

	p.closed = true

	p.mu.Unlock()

	return nil
}

// Helper function for reusing the existing connection
func (p *RpcConnPool) reuseConnLocked() (*rpcConn, error) {
	for _, c := range p.idleConns {
		ok := c.idleTimer.Stop()
		if ok {
			c.stopTimerChan <- true
		}
		delete(p.idleConns, c.id)
		p.mu.Unlock()
		return c, nil
	}
	return nil, errors.New("no Idle connection")
}
