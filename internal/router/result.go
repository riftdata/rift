package router

import (
	"fmt"
	"strconv"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

		if err := sendDataRow(client, values); err != nil {
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

		// Format code (0 = text, 1 = binary)
		buf.WriteInt16(f.Format)
	}

	return client.WriteMessage(pgwire.MsgRowDescription, buf.Bytes())
}

// sendDataRow builds and sends a DataRow ('D') message.
// Values are sent in text format.
func sendDataRow(client *pgwire.ClientConn, values []interface{}) error {
	buf := pgwire.NewBuffer(256)

	// Number of columns
	buf.WriteInt16(int16(len(values))) // #nosec G115 -- column count fits in int16

	for _, v := range values {
		if v == nil {
			// NULL value: -1 length
			buf.WriteInt32(-1)
			continue
		}

		// Convert to text representation
		text := formatValue(v)
		textBytes := []byte(text)
		buf.WriteInt32(int32(len(textBytes))) // #nosec G115 -- text length fits in int32
		buf.WriteBytes(textBytes)
	}

	return client.WriteMessage(pgwire.MsgDataRow, buf.Bytes())
}

// formatValue converts a Go value to its Postgres text representation.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
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
	default:
		return fmt.Sprintf("%v", val)
	}
}
