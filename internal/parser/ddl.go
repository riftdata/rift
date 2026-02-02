package parser

// DDLInfo holds metadata about a DDL operation for branch tracking.
type DDLInfo struct {
	Type      DDLType
	TableName string
	Schema    string
}

// ExtractDDLInfo returns DDL metadata from a parsed query.
// Returns nil if the query is not a DDL statement.
func ExtractDDLInfo(pq *ParsedQuery) *DDLInfo {
	if pq.Type != QueryDDL {
		return nil
	}

	info := &DDLInfo{
		Type: pq.DDLType,
	}

	if len(pq.Tables) > 0 {
		info.TableName = pq.Tables[0].Name
		info.Schema = pq.Tables[0].Schema
		if info.Schema == "" {
			info.Schema = "public"
		}
	}

	return info
}

// IsTableDDL returns true if the DDL affects a table (CREATE/ALTER/DROP TABLE).
func IsTableDDL(pq *ParsedQuery) bool {
	if pq.Type != QueryDDL {
		return false
	}
	return pq.DDLType == DDLCreateTable || pq.DDLType == DDLAlterTable || pq.DDLType == DDLDropTable
}
