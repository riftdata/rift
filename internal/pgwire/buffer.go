package pgwire

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var (
	ErrBufferTooSmall  = errors.New("buffer too small")
	ErrInvalidMessage  = errors.New("invalid message")
	ErrMessageTooLarge = errors.New("message too large")
)

const (
	MaxMessageSize = 1 << 30 // 1GB max message size
)

// Buffer is a reusable buffer for reading and writing Postgres messages
type Buffer struct {
	buf []byte
	pos int
}

// NewBuffer creates a new buffer with the given initial capacity
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		buf: make([]byte, 0, capacity),
	}
}

// Reset clears the buffer for reuse
func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
	b.pos = 0
}

// Bytes return the buffer contents
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// Len returns the length of data in the buffer
func (b *Buffer) Len() int {
	return len(b.buf)
}

// Remaining returns bytes remaining to read
func (b *Buffer) Remaining() int {
	return len(b.buf) - b.pos
}

// Position returns current read position
func (b *Buffer) Position() int {
	return b.pos
}

// SetPosition sets the read position
func (b *Buffer) SetPosition(pos int) {
	b.pos = pos
}

// --- Writing ---

// WriteByte appends a single byte (implements io.ByteWriter)
func (b *Buffer) WriteByte(v byte) error {
	b.buf = append(b.buf, v)
	return nil
}

// WriteInt16 appends a 16-bit integer (big-endian)
func (b *Buffer) WriteInt16(v int16) {
	b.buf = append(b.buf, byte(v>>8), byte(v))
}

// WriteUint16 appends an unsigned 16-bit integer (big-endian)
func (b *Buffer) WriteUint16(v uint16) {
	b.buf = append(b.buf, byte(v>>8), byte(v))
}

// WriteInt32 appends a 32-bit integer (big-endian)
func (b *Buffer) WriteInt32(v int32) {
	b.buf = append(b.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteUint32 appends an unsigned 32-bit integer (big-endian)
func (b *Buffer) WriteUint32(v uint32) {
	b.buf = append(b.buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteInt64 appends a 64-bit integer (big-endian)
func (b *Buffer) WriteInt64(v int64) {
	b.buf = append(b.buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteBytes appends raw bytes
func (b *Buffer) WriteBytes(v []byte) {
	b.buf = append(b.buf, v...)
}

// WriteString appends a null-terminated string
func (b *Buffer) WriteString(s string) {
	b.buf = append(b.buf, s...)
	b.buf = append(b.buf, 0)
}

// WriteRawString appends a string without a null terminator
func (b *Buffer) WriteRawString(s string) {
	b.buf = append(b.buf, s...)
}

// --- Reading ---

// ReadByte reads a single byte
func (b *Buffer) ReadByte() (byte, error) {
	if b.pos >= len(b.buf) {
		return 0, io.EOF
	}
	v := b.buf[b.pos]
	b.pos++
	return v, nil
}

// ReadInt16 reads a 16-bit integer (big-endian)
func (b *Buffer) ReadInt16() (int16, error) {
	if b.pos+2 > len(b.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int16(b.buf[b.pos])<<8 | int16(b.buf[b.pos+1])
	b.pos += 2
	return v, nil
}

// ReadUint16 reads an unsigned 16-bit integer (big-endian)
func (b *Buffer) ReadUint16() (uint16, error) {
	if b.pos+2 > len(b.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint16(b.buf[b.pos])<<8 | uint16(b.buf[b.pos+1])
	b.pos += 2
	return v, nil
}

// ReadInt32 reads a 32-bit integer (big-endian)
func (b *Buffer) ReadInt32() (int32, error) {
	if b.pos+4 > len(b.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int32(b.buf[b.pos])<<24 | int32(b.buf[b.pos+1])<<16 |
		int32(b.buf[b.pos+2])<<8 | int32(b.buf[b.pos+3])
	b.pos += 4
	return v, nil
}

// ReadUint32 reads an unsigned 32-bit integer (big-endian)
func (b *Buffer) ReadUint32() (uint32, error) {
	if b.pos+4 > len(b.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := uint32(b.buf[b.pos])<<24 | uint32(b.buf[b.pos+1])<<16 |
		uint32(b.buf[b.pos+2])<<8 | uint32(b.buf[b.pos+3])
	b.pos += 4
	return v, nil
}

// ReadInt64 reads a 64-bit integer (big-endian)
func (b *Buffer) ReadInt64() (int64, error) {
	if b.pos+8 > len(b.buf) {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(b.buf[b.pos])<<56 | int64(b.buf[b.pos+1])<<48 |
		int64(b.buf[b.pos+2])<<40 | int64(b.buf[b.pos+3])<<32 |
		int64(b.buf[b.pos+4])<<24 | int64(b.buf[b.pos+5])<<16 |
		int64(b.buf[b.pos+6])<<8 | int64(b.buf[b.pos+7])
	b.pos += 8
	return v, nil
}

// ReadBytes reads n bytes
func (b *Buffer) ReadBytes(n int) ([]byte, error) {
	if b.pos+n > len(b.buf) {
		return nil, io.ErrUnexpectedEOF
	}
	v := b.buf[b.pos : b.pos+n]
	b.pos += n
	return v, nil
}

// ReadString reads a null-terminated string
func (b *Buffer) ReadString() (string, error) {
	start := b.pos
	for b.pos < len(b.buf) {
		if b.buf[b.pos] == 0 {
			s := string(b.buf[start:b.pos])
			b.pos++ // skip null terminator
			return s, nil
		}
		b.pos++
	}
	return "", io.ErrUnexpectedEOF
}

// ReadRemainder reads all remaining bytes
func (b *Buffer) ReadRemainder() []byte {
	v := b.buf[b.pos:]
	b.pos = len(b.buf)
	return v
}

// --- Message I/O ---

// ReadMessage reads a complete Postgres message from the reader
// Returns message type and payload (without type byte and length)
func ReadMessage(r io.Reader) (msgType byte, payload []byte, err error) {
	// Read type byte
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}

	msgType = header[0]
	length := int(binary.BigEndian.Uint32(header[1:5])) - 4 // length includes itself

	if length < 0 || length > MaxMessageSize {
		return 0, nil, ErrMessageTooLarge
	}

	// Read payload
	payload = make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, err
		}
	}

	return msgType, payload, nil
}

// ReadStartupMessage reads the initial startup message (no type byte)
func ReadStartupMessage(r io.Reader) ([]byte, error) {
	// Read length (4 bytes)
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(header)) - 4

	if length < 0 || length > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	// Read payload
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
	}

	return payload, nil
}

// WriteMessage writes a complete Postgres message
func WriteMessage(w io.Writer, msgType byte, payload []byte) error {
	length := len(payload) + 4 // length includes itself

	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:], uint32(length)) // #nosec G115 -- length is validated above

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// WriteRaw writes raw bytes (for startup messages)
func WriteRaw(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}

// --- Helper functions for building specific messages ---

// BuildErrorResponse creates an ErrorResponse message payload
func BuildErrorResponse(severity, code, message string) []byte {
	buf := NewBuffer(256)

	_ = buf.WriteByte(FieldSeverity)
	buf.WriteString(severity)

	_ = buf.WriteByte(FieldCode)
	buf.WriteString(code)

	_ = buf.WriteByte(FieldMessage)
	buf.WriteString(message)

	_ = buf.WriteByte(0) // terminator

	return buf.Bytes()
}

// BuildNoticeResponse creates a NoticeResponse message payload
func BuildNoticeResponse(severity, code, message string) []byte {
	// Same format as error response
	return BuildErrorResponse(severity, code, message)
}

// BuildReadyForQuery creates a ReadyForQuery message payload
func BuildReadyForQuery(txStatus byte) []byte {
	return []byte{txStatus}
}

// BuildParameterStatus creates a ParameterStatus message payload
func BuildParameterStatus(name, value string) []byte {
	buf := NewBuffer(64)
	buf.WriteString(name)
	buf.WriteString(value)
	return buf.Bytes()
}

// BuildAuthenticationOk creates an AuthenticationOk message payload
func BuildAuthenticationOk() []byte {
	buf := NewBuffer(4)
	buf.WriteInt32(AuthOK)
	return buf.Bytes()
}

// BuildAuthenticationMD5 creates an AuthenticationMD5Password message payload
func BuildAuthenticationMD5(salt [4]byte) []byte {
	buf := NewBuffer(8)
	buf.WriteInt32(AuthMD5Password)
	buf.WriteBytes(salt[:])
	return buf.Bytes()
}

// BuildAuthenticationCleartext creates an AuthenticationCleartextPassword payload
func BuildAuthenticationCleartext() []byte {
	buf := NewBuffer(4)
	buf.WriteInt32(AuthCleartextPassword)
	return buf.Bytes()
}

// BuildBackendKeyData creates a BackendKeyData message payload
func BuildBackendKeyData(pid, secretKey int32) []byte {
	buf := NewBuffer(8)
	buf.WriteInt32(pid)
	buf.WriteInt32(secretKey)
	return buf.Bytes()
}

// BuildCommandComplete creates a CommandComplete message payload
func BuildCommandComplete(tag string) []byte {
	buf := NewBuffer(len(tag) + 1)
	buf.WriteString(tag)
	return buf.Bytes()
}

// ParseStartupMessage parses startup message parameters
func ParseStartupMessage(payload []byte) (version int32, params map[string]string, err error) {
	if len(payload) < 4 {
		return 0, nil, ErrInvalidMessage
	}

	version = int32(binary.BigEndian.Uint32(payload[:4])) // #nosec G115 -- protocol version fits in int32
	params = make(map[string]string)

	// Parse key-value pairs (null-terminated strings)
	buf := NewBuffer(0)
	buf.buf = payload[4:]

	for buf.Remaining() > 1 {
		key, err := buf.ReadString()
		if err != nil || key == "" {
			break
		}
		value, err := buf.ReadString()
		if err != nil {
			break
		}
		params[key] = value
	}

	return version, params, nil
}

// Float32 conversion helpers (bitwise reinterpretation, not lossy conversion)

func Float32ToInt32Bits(f float32) int32 {
	return int32(math.Float32bits(f)) // #nosec G115 -- bitwise reinterpretation, not arithmetic conversion
}

func Int32BitsToFloat32(i int32) float32 {
	return math.Float32frombits(uint32(i)) // #nosec G115 -- bitwise reinterpretation, not arithmetic conversion
}

func Float64ToInt64Bits(f float64) int64 {
	return int64(math.Float64bits(f)) // #nosec G115 -- bitwise reinterpretation, not arithmetic conversion
}

func Int64BitsToFloat64(i int64) float64 {
	return math.Float64frombits(uint64(i)) // #nosec G115 -- bitwise reinterpretation, not arithmetic conversion
}
