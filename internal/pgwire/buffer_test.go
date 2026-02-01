package pgwire

import (
	"bytes"
	"testing"
)

func TestBufferWriteRead(t *testing.T) {
	buf := NewBuffer(64)

	// Write values
	_ = buf.WriteByte(42)
	buf.WriteInt16(1234)
	buf.WriteInt32(567890)
	buf.WriteString("hello")
	buf.WriteBytes([]byte{1, 2, 3})

	// Read back
	buf.SetPosition(0)

	b, err := buf.ReadByte()
	if err != nil || b != 42 {
		t.Errorf("ReadByte: got %d, want 42", b)
	}

	i16, err := buf.ReadInt16()
	if err != nil || i16 != 1234 {
		t.Errorf("ReadInt16: got %d, want 1234", i16)
	}

	i32, err := buf.ReadInt32()
	if err != nil || i32 != 567890 {
		t.Errorf("ReadInt32: got %d, want 567890", i32)
	}

	s, err := buf.ReadString()
	if err != nil || s != "hello" {
		t.Errorf("ReadString: got %q, want 'hello'", s)
	}

	data, err := buf.ReadBytes(3)
	if err != nil || !bytes.Equal(data, []byte{1, 2, 3}) {
		t.Errorf("ReadBytes: got %v, want [1 2 3]", data)
	}
}

func TestParseStartupMessage(t *testing.T) {
	buf := NewBuffer(256)
	buf.WriteInt32(ProtocolVersionNumber)
	buf.WriteString("user")
	buf.WriteString("testuser")
	buf.WriteString("database")
	buf.WriteString("testdb")
	_ = buf.WriteByte(0)

	version, params, err := ParseStartupMessage(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseStartupMessage: %v", err)
	}

	if version != ProtocolVersionNumber {
		t.Errorf("version: got %d, want %d", version, ProtocolVersionNumber)
	}

	if params["user"] != "testuser" {
		t.Errorf("user: got %q, want 'testuser'", params["user"])
	}

	if params["database"] != "testdb" {
		t.Errorf("database: got %q, want 'testdb'", params["database"])
	}
}

func TestBuildErrorResponse(t *testing.T) {
	payload := BuildErrorResponse("ERROR", "42P01", "table not found")

	buf := NewBuffer(0)
	buf.WriteBytes(payload)
	buf.SetPosition(0)

	var severity, code, message string

	for {
		fieldType, err := buf.ReadByte()
		if err != nil || fieldType == 0 {
			break
		}
		value, _ := buf.ReadString()
		switch fieldType {
		case FieldSeverity:
			severity = value
		case FieldCode:
			code = value
		case FieldMessage:
			message = value
		}
	}

	if severity != "ERROR" {
		t.Errorf("severity: got %q, want 'ERROR'", severity)
	}
	if code != "42P01" {
		t.Errorf("code: got %q, want '42P01'", code)
	}
	if message != "table not found" {
		t.Errorf("message: got %q, want 'table not found'", message)
	}
}

func TestMD5Password(t *testing.T) {
	// Known test case
	user := "postgres"
	pass := "secret"
	salt := [4]byte{0x01, 0x02, 0x03, 0x04}

	result := MD5Password(user, pass, salt)

	// MD5 should always start with "md5"
	if len(result) < 3 || result[:3] != "md5" {
		t.Errorf("MD5Password should start with 'md5', got %q", result)
	}

	// The result should be "md5" + 32 hex chars = 35 chars total
	if len(result) != 35 {
		t.Errorf("MD5Password length: got %d, want 35", len(result))
	}
}
