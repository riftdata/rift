package router

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/riftdata/rift/internal/pgwire"
)

// sendQueryResult serializes pgx rows back to Postgres wire protocol and writes
// them to the client connection. This converts the pgx result set into
// RowDescription + DataRow* + CommandComplete messages.
func sendQueryResult(client *pgwire.ClientConn, rows pgx.Rows, tag string) error {
	defer rows.Close()

	// Send RowDescription
	fieldDescs := rows.FieldDescriptions()
	if err := sendRowDescription(client, fieldDescs); err != nil {
		return fmt.Errorf("send row description: %w", err)
	}

	// Send DataRows
	rowCount := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row values: %w", err)
		}

		if err := sendDataRow(client, values, fieldDescs); err != nil {
			return fmt.Errorf("send data row: %w", err)
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	// Send CommandComplete
	cmdTag := tag
	if tag == "" {
		cmdTag = fmt.Sprintf("SELECT %d", rowCount)
	}
	return client.SendCommandComplete(cmdTag)
}

// sendRowDescription builds and sends a RowDescription ('T') message.
func sendRowDescription(client *pgwire.ClientConn, fields []pgconn.FieldDescription) error {
	buf := pgwire.NewBuffer(256)

	// Number of fields
	buf.WriteInt16(int16(len(fields))) // #nosec G115 -- field count fits in int16

	for _, f := range fields {
		// Field name (null-terminated)
		buf.WriteString(f.Name)

		// Table OID (0 if not from a table)
		buf.WriteInt32(int32(f.TableOID)) // #nosec G115 -- OID fits in int32

		// Column attribute number
		buf.WriteInt16(int16(f.TableAttributeNumber)) // #nosec G115 -- attribute number fits in int16

		// Data type OID
		buf.WriteInt32(int32(f.DataTypeOID)) // #nosec G115 -- OID fits in int32

		// Data type size
		buf.WriteInt16(f.DataTypeSize)

		// Type modifier
		buf.WriteInt32(f.TypeModifier)

		// Format code — always text (0) since sendDataRow emits text values
		buf.WriteInt16(0)
	}

	return client.WriteMessage(pgwire.MsgRowDescription, buf.Bytes())
}

// sendDataRow builds and sends a DataRow ('D') message.
// Values are sent in text format using OID-aware encoding.
func sendDataRow(client *pgwire.ClientConn, values []interface{}, fields []pgconn.FieldDescription) error {
	buf := pgwire.NewBuffer(256)

	// Number of columns
	buf.WriteInt16(int16(len(values))) // #nosec G115 -- column count fits in int16

	for i, v := range values {
		if v == nil {
			// NULL value: -1 length
			buf.WriteInt32(-1)
			continue
		}

		var oid uint32
		if i < len(fields) {
			oid = fields[i].DataTypeOID
		}

		// Convert to text representation using OID
		text := formatValue(v, oid)
		textBytes := []byte(text)
		buf.WriteInt32(int32(len(textBytes))) // #nosec G115 -- text length fits in int32
		buf.WriteBytes(textBytes)
	}

	return client.WriteMessage(pgwire.MsgDataRow, buf.Bytes())
}

// formatValue converts a Go value to its Postgres text wire representation,
// using the column OID to select the correct encoding.
func formatValue(v interface{}, oid uint32) string {
	if s, ok := formatByOID(v, oid); ok {
		return s
	}
	return formatByType(v)
}

// timeFormatByOID maps time-related OIDs to their Postgres text layout.
var timeFormatByOID = map[uint32]string{
	pgtype.TimestampOID:   "2006-01-02 15:04:05.999999",
	pgtype.TimestamptzOID: "2006-01-02 15:04:05.999999-07",
	pgtype.DateOID:        "2006-01-02",
	pgtype.TimeOID:        "15:04:05.999999",
	pgtype.TimetzOID:      "15:04:05.999999",
}

// formatByOID attempts OID-specific encoding. Returns (result, true) on match.
func formatByOID(v interface{}, oid uint32) (string, bool) {
	if layout, ok := timeFormatByOID[oid]; ok {
		if t, ok := v.(time.Time); ok {
			return t.Format(layout), true
		}
		return "", false
	}
	switch oid {
	case pgtype.ByteaOID:
		if b, ok := v.([]byte); ok {
			return `\x` + hex.EncodeToString(b), true
		}
	case pgtype.UUIDOID:
		switch val := v.(type) {
		case [16]byte:
			return fmt.Sprintf("%x-%x-%x-%x-%x", val[0:4], val[4:6], val[6:8], val[8:10], val[10:16]), true
		case string:
			return val, true
		}
	case pgtype.NumericOID:
		switch val := v.(type) {
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64), true
		case float32:
			return strconv.FormatFloat(float64(val), 'f', -1, 32), true
		case string:
			return val, true
		}
	}
	return "", false
}

// formatByType encodes a Go value to Postgres text using its Go type.
func formatByType(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return `\x` + hex.EncodeToString(val)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "t"
		}
		return "f"
	case time.Time:
		return val.Format("2006-01-02 15:04:05.999999-07")
	default:
		return fmt.Sprintf("%v", val)
	}
}
