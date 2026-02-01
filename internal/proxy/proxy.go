package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riftdata/rift/internal/pgwire"
)

var (
	ErrProxyClosed    = errors.New("proxy server closed")
	ErrUpstreamClosed = errors.New("upstream connection closed")
	ErrBranchNotFound = errors.New("branch not found")
)

// Config holds proxy configuration
type Config struct {
	ListenAddr     string
	UpstreamAddr   string
	UpstreamUser   string
	UpstreamPass   string
	MaxConnections int
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
}

// DefaultConfig returns default proxy configuration
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:     ":6432",
		MaxConnections: 100,
		ConnectTimeout: 10 * time.Second,
		IdleTimeout:    5 * time.Minute,
	}
}

// Proxy is the main Postgres proxy server
type Proxy struct {
	config   *Config
	listener net.Listener

	// Connection tracking
	connections sync.Map // ConnID -> *clientSession
	connCount   atomic.Int64

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool

	// Hooks for branch routing (to be set by branch manager)
	OnConnect    func(database string) (upstreamDB string, err error)
	Authenticate func(user, database, password string) error
}

// clientSession holds state for a single client connection
type clientSession struct {
	client   *pgwire.ClientConn
	upstream net.Conn
	branch   string
}

// New creates a new proxy server
func New(config *Config) *Proxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &Proxy{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start starts the proxy server
func (p *Proxy) Start() error {
	listener, err := net.Listen("tcp", p.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.config.ListenAddr, err)
	}
	p.listener = listener

	p.wg.Add(1)
	go p.acceptLoop()

	return nil
}

// Stop gracefully stops the proxy server
func (p *Proxy) Stop() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	p.cancel()

	if p.listener != nil {
		_ = p.listener.Close()
	}

	// Close all client connections
	p.connections.Range(func(key, value interface{}) bool {
		if session, ok := value.(*clientSession); ok {
			_ = session.client.Close()
			if session.upstream != nil {
				_ = session.upstream.Close()
			}
		}
		return true
	})

	p.wg.Wait()
	return nil
}

// Addr returns the listener address
func (p *Proxy) Addr() net.Addr {
	if p.listener == nil {
		return nil
	}
	return p.listener.Addr()
}

// ConnectionCount returns the number of active connections
func (p *Proxy) ConnectionCount() int64 {
	return p.connCount.Load()
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.ctx.Done():
				return
			default:
				// Log error and continue
				fmt.Printf("accept error: %v\n", err)
				continue
			}
		}

		// Check max connections
		if p.config.MaxConnections > 0 && p.connCount.Load() >= int64(p.config.MaxConnections) {
			_ = conn.Close()
			continue
		}

		p.wg.Add(1)
		go p.handleConnection(conn)
	}
}

func (p *Proxy) handleConnection(conn net.Conn) {
	defer p.wg.Done()

	client := pgwire.NewClientConn(conn)
	p.connCount.Add(1)
	defer func() {
		p.connCount.Add(-1)
		p.connections.Delete(client.ID())
		_ = client.Close()
	}()

	// Perform handshake
	if err := client.Handshake(p.Authenticate); err != nil {
		fmt.Printf("handshake error: %v\n", err)
		return
	}

	// Resolve database to upstream (branch routing)
	database := client.Database()
	upstreamDB := database
	if p.OnConnect != nil {
		var err error
		upstreamDB, err = p.OnConnect(database)
		if err != nil {
			_ = client.SendError("FATAL", pgwire.ErrCodeInvalidCatalogName, err.Error())
			return
		}
	}

	// Connect to upstream
	upstream, err := p.connectUpstream(upstreamDB, client.User())
	if err != nil {
		_ = client.SendError("FATAL", pgwire.ErrCodeConnectionFailure, fmt.Sprintf("upstream connection failed: %v", err))
		return
	}
	defer func() { _ = upstream.Close() }()

	// Track session
	session := &clientSession{
		client:   client,
		upstream: upstream,
		branch:   database,
	}
	p.connections.Store(client.ID(), session)

	// Start proxying
	p.proxyTraffic(client, upstream)
}

func (p *Proxy) connectUpstream(database, user string) (net.Conn, error) {
	// Connect to upstream Postgres
	conn, err := net.DialTimeout("tcp", p.config.UpstreamAddr, p.config.ConnectTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial upstream: %w", err)
	}

	// Send startup message
	startup := buildStartupMessage(database, user, p.config.UpstreamUser)
	if _, err := conn.Write(startup); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("send startup: %w", err)
	}

	// Handle authentication
	if err := p.handleUpstreamAuth(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("upstream auth: %w", err)
	}

	return conn, nil
}

func buildStartupMessage(database, clientUser, upstreamUser string) []byte {
	buf := pgwire.NewBuffer(256)

	// Placeholder for length (will be filled in)
	buf.WriteInt32(0)

	// Protocol version 3.0
	buf.WriteInt32(pgwire.ProtocolVersionNumber)

	// Parameters
	user := upstreamUser
	if user == "" {
		user = clientUser
	}

	buf.WriteString("user")
	buf.WriteString(user)

	buf.WriteString("database")
	buf.WriteString(database)

	buf.WriteString("application_name")
	buf.WriteString("rift")

	// Terminator
	buf.WriteByte(0)

	// Fill in length
	data := buf.Bytes()
	length := len(data)
	data[0] = byte(length >> 24)
	data[1] = byte(length >> 16)
	data[2] = byte(length >> 8)
	data[3] = byte(length)

	return data
}

func (p *Proxy) handleUpstreamAuth(conn net.Conn) error {
	for {
		msgType, payload, err := pgwire.ReadMessage(conn)
		if err != nil {
			return err
		}

		switch msgType {
		case pgwire.MsgAuthentication:
			if len(payload) < 4 {
				return errors.New("invalid auth message")
			}
			authType := int32(payload[0])<<24 | int32(payload[1])<<16 | int32(payload[2])<<8 | int32(payload[3])

			switch authType {
			case pgwire.AuthOK:
				// Continue to receive parameter status messages
				continue

			case pgwire.AuthCleartextPassword:
				// Send password
				passBuf := pgwire.NewBuffer(64)
				passBuf.WriteString(p.config.UpstreamPass)
				if err := pgwire.WriteMessage(conn, pgwire.MsgPassword, passBuf.Bytes()); err != nil {
					return err
				}

			case pgwire.AuthMD5Password:
				if len(payload) < 8 {
					return errors.New("invalid MD5 auth message")
				}
				var salt [4]byte
				copy(salt[:], payload[4:8])
				hash := pgwire.MD5Password(p.config.UpstreamUser, p.config.UpstreamPass, salt)

				passBuf := pgwire.NewBuffer(64)
				passBuf.WriteString(hash)
				if err := pgwire.WriteMessage(conn, pgwire.MsgPassword, passBuf.Bytes()); err != nil {
					return err
				}

			default:
				return fmt.Errorf("%w: type %d", pgwire.ErrUnsupportedAuth, authType)
			}

		case pgwire.MsgParameterStatus:
			// Ignore parameter status messages
			continue

		case pgwire.MsgBackendKeyData:
			// Store for cancel requests if needed
			continue

		case pgwire.MsgReadyForQuery:
			// Authentication complete
			return nil

		case pgwire.MsgErrorResponse:
			// Parse error
			return parseError(payload)

		default:
			return fmt.Errorf("unexpected message type during auth: %c", msgType)
		}
	}
}

func parseError(payload []byte) error {
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload) // Hacky way to set buf content
	buf.SetPosition(0)

	var message string
	for {
		fieldType, err := buf.ReadByte()
		if err != nil || fieldType == 0 {
			break
		}
		value, err := buf.ReadString()
		if err != nil {
			break
		}
		if fieldType == pgwire.FieldMessage {
			message = value
		}
	}

	if message == "" {
		message = "unknown error"
	}
	return errors.New(message)
}

func (p *Proxy) proxyTraffic(client *pgwire.ClientConn, upstream net.Conn) {
	ctx, cancel := context.WithCancel(p.ctx)
	defer cancel()

	errCh := make(chan error, 2)

	// Client -> Upstream
	go func() {
		errCh <- p.copyClientToUpstream(ctx, client.NetConn(), upstream)
	}()

	// Upstream -> Client
	go func() {
		errCh <- p.copyUpstreamToClient(ctx, upstream, client.NetConn())
	}()

	// Wait for either direction to finish
	<-errCh
	cancel() // Stop the other direction
	<-errCh  // Wait for it to finish
}

func (p *Proxy) copyClientToUpstream(ctx context.Context, client, upstream net.Conn) error {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Set read deadline
		_ = client.SetReadDeadline(time.Now().Add(p.config.IdleTimeout))

		n, err := client.Read(buf)
		if err != nil {
			return err
		}

		// TODO: Intercept and potentially rewrite queries here
		// For now, pass through directly

		if _, err := upstream.Write(buf[:n]); err != nil {
			return err
		}
	}
}

func (p *Proxy) copyUpstreamToClient(ctx context.Context, upstream, client net.Conn) error {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := upstream.Read(buf)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// TODO: Intercept and potentially modify results here
		// For now, pass through directly

		if _, err := client.Write(buf[:n]); err != nil {
			return err
		}
	}
}
