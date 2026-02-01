package pgwire

import (
	"crypto/md5" //nolint:gosec // required by Postgres wire protocol
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSSLNotSupported      = errors.New("SSL not supported")
	ErrInvalidStartup       = errors.New("invalid startup message")
	ErrAuthenticationFailed = errors.New("authentication failed")
	ErrUnsupportedAuth      = errors.New("unsupported authentication method")
	ErrConnectionClosed     = errors.New("connection closed")
)

// ConnID is a unique connection identifier
type ConnID uint64

var connIDCounter uint64

func nextConnID() ConnID {
	return ConnID(atomic.AddUint64(&connIDCounter, 1))
}

// ClientConn represents a client connection to the proxy
type ClientConn struct {
	id        ConnID
	conn      net.Conn
	params    map[string]string
	database  string
	user      string
	pid       int32
	secretKey int32

	mu     sync.Mutex
	closed bool

	// Read/write buffers
	readBuf  *Buffer
	writeBuf *Buffer
}

// NewClientConn creates a new client connection wrapper
func NewClientConn(conn net.Conn) *ClientConn {
	// Generate random PID and secret key for this connection
	var pidBytes, keyBytes [4]byte
	_, _ = rand.Read(pidBytes[:])
	_, _ = rand.Read(keyBytes[:])

	return &ClientConn{
		id:        nextConnID(),
		conn:      conn,
		params:    make(map[string]string),
		pid:       int32(pidBytes[0])<<24 | int32(pidBytes[1])<<16 | int32(pidBytes[2])<<8 | int32(pidBytes[3]),
		secretKey: int32(keyBytes[0])<<24 | int32(keyBytes[1])<<16 | int32(keyBytes[2])<<8 | int32(keyBytes[3]),
		readBuf:   NewBuffer(4096),
		writeBuf:  NewBuffer(4096),
	}
}

// ID returns the connection ID
func (c *ClientConn) ID() ConnID {
	return c.id
}

// Database returns the database name
func (c *ClientConn) Database() string {
	return c.database
}

// User returns the username
func (c *ClientConn) User() string {
	return c.user
}

// Params returns startup parameters
func (c *ClientConn) Params() map[string]string {
	return c.params
}

// RemoteAddr returns the client's remote address
func (c *ClientConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the connection
func (c *ClientConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// SetDeadline sets read/write deadline
func (c *ClientConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

// Handshake performs the initial Postgres handshake
// Returns nil on successful authentication
func (c *ClientConn) Handshake(authenticate func(user, database, password string) error) error {
	version, params, err := c.readStartup()
	if err != nil {
		return err
	}

	// Check protocol version
	if version != ProtocolVersionNumber {
		return fmt.Errorf("%w: unsupported protocol version %d", ErrInvalidStartup, version)
	}

	// Extract connection parameters
	c.params = params
	c.user = params["user"]
	c.database = params["database"]

	if c.database == "" {
		c.database = c.user // Default database is username
	}

	// Perform authentication
	if authenticate != nil {
		if err := c.authenticateClient(authenticate); err != nil {
			return err
		}
	}

	return c.sendPostAuthMessages()
}

// readStartup reads the startup message, handling SSL and GSSENC negotiation.
func (c *ClientConn) readStartup() (version int32, params map[string]string, err error) {
	var payload []byte
	payload, err = ReadStartupMessage(c.conn)
	if err != nil {
		return 0, nil, fmt.Errorf("reading startup: %w", err)
	}

	version, params, err = ParseStartupMessage(payload)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing startup: %w", err)
	}

	// Handle SSL and GSSENC requests by declining and re-reading
	for version == SSLRequestCode || version == GSSENCRequestCode {
		if _, err = c.conn.Write([]byte{'N'}); err != nil {
			return 0, nil, err
		}
		payload, err = ReadStartupMessage(c.conn)
		if err != nil {
			return 0, nil, fmt.Errorf("reading startup after negotiation: %w", err)
		}
		version, params, err = ParseStartupMessage(payload)
		if err != nil {
			return 0, nil, fmt.Errorf("parsing startup after negotiation: %w", err)
		}
	}

	return version, params, nil
}

// authenticateClient performs cleartext password authentication.
func (c *ClientConn) authenticateClient(authenticate func(user, database, password string) error) error {
	if err := c.requestCleartextPassword(); err != nil {
		return err
	}

	msgType, payload, err := ReadMessage(c.conn)
	if err != nil {
		return fmt.Errorf("reading password: %w", err)
	}

	if msgType != MsgPassword {
		return fmt.Errorf("%w: expected password message, got %c", ErrInvalidStartup, msgType)
	}

	password := strings.TrimSuffix(string(payload), "\x00")

	if err := authenticate(c.user, c.database, password); err != nil {
		_ = c.sendError("FATAL", ErrCodeInsufficientPrivilege, "authentication failed")
		return ErrAuthenticationFailed
	}
	return nil
}

// sendPostAuthMessages sends the standard post-authentication messages to the client.
func (c *ClientConn) sendPostAuthMessages() error {
	if err := WriteMessage(c.conn, MsgAuthentication, BuildAuthenticationOk()); err != nil {
		return err
	}

	if err := WriteMessage(c.conn, MsgBackendKeyData, BuildBackendKeyData(c.pid, c.secretKey)); err != nil {
		return err
	}

	serverParams := map[string]string{
		"server_version":              "15.0 (Rift)",
		"server_encoding":             "UTF8",
		"client_encoding":             "UTF8",
		"DateStyle":                   "ISO, MDY",
		"TimeZone":                    "UTC",
		"integer_datetimes":           "on",
		"standard_conforming_strings": "on",
	}

	for name, value := range serverParams {
		if err := WriteMessage(c.conn, MsgParameterStatus, BuildParameterStatus(name, value)); err != nil {
			return err
		}
	}

	return WriteMessage(c.conn, MsgReadyForQuery, BuildReadyForQuery(TxStatusIdle))
}

func (c *ClientConn) requestCleartextPassword() error {
	return WriteMessage(c.conn, MsgAuthentication, BuildAuthenticationCleartext())
}

// ReadMessage reads the next message from the client
func (c *ClientConn) ReadMessage() (msgType byte, payload []byte, err error) {
	return ReadMessage(c.conn)
}

// WriteMessage writes a message to the client
func (c *ClientConn) WriteMessage(msgType byte, payload []byte) error {
	return WriteMessage(c.conn, msgType, payload)
}

// WriteRaw writes raw bytes to the client
func (c *ClientConn) WriteRaw(data []byte) error {
	_, err := c.conn.Write(data)
	return err
}

// sendError sends an error response to the client
func (c *ClientConn) sendError(severity, code, message string) error {
	return WriteMessage(c.conn, MsgErrorResponse, BuildErrorResponse(severity, code, message))
}

// SendError sends an error response
func (c *ClientConn) SendError(severity, code, message string) error {
	return c.sendError(severity, code, message)
}

// SendNotice sends a notice response
func (c *ClientConn) SendNotice(severity, code, message string) error {
	return WriteMessage(c.conn, MsgNoticeResponse, BuildNoticeResponse(severity, code, message))
}

// SendReadyForQuery sends a ReadyForQuery message
func (c *ClientConn) SendReadyForQuery(txStatus byte) error {
	return WriteMessage(c.conn, MsgReadyForQuery, BuildReadyForQuery(txStatus))
}

// SendCommandComplete sends a CommandComplete message
func (c *ClientConn) SendCommandComplete(tag string) error {
	return WriteMessage(c.conn, MsgCommandComplete, BuildCommandComplete(tag))
}

// MD5Password computes the MD5 password hash per Postgres wire protocol.
// MD5 is required by the protocol specification; this is not a choice of hash.
func MD5Password(user, password string, salt [4]byte) string {
	// concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
	inner := md5.Sum([]byte(password + user)) //nolint:gosec // required by Postgres wire protocol
	innerHex := hex.EncodeToString(inner[:])
	outer := md5.Sum(append([]byte(innerHex), salt[:]...)) //nolint:gosec // required by Postgres wire protocol
	return "md5" + hex.EncodeToString(outer[:])
}

// NetConn returns the underlying net.Conn
func (c *ClientConn) NetConn() net.Conn {
	return c.conn
}
