package parser

import (
	"fmt"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// QueryType classifies the kind of SQL statement.
type QueryType int

const (
	QueryUnknown QueryType = iota
	QuerySelect
	QueryInsert
	QueryUpdate
	QueryDelete
	QueryDDL
	QueryUtility // SET, SHOW, BEGIN, COMMIT, ROLLBACK, etc.
)

func (q QueryType) String() string {
	switch q {
	case QuerySelect:
		return "SELECT"
	case QueryInsert:
		return "INSERT"
	case QueryUpdate:
		return "UPDATE"
	case QueryDelete:
		return "DELETE"
	case QueryDDL:
		return "DDL"
	case QueryUtility:
		return "UTILITY"
	default:
		return "UNKNOWN"
	}
}

// TableRef identifies a table referenced in a query.
type TableRef struct {
	Schema string
	Name   string
	Alias  string
}

// QualifiedName returns schema.table or just table if no schema.
func (t TableRef) QualifiedName() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	return t.Name
}

// DDLType classifies DDL operations.
type DDLType int

const (
	DDLNone DDLType = iota
	DDLCreateTable
	DDLAlterTable
	DDLDropTable
	DDLCreateIndex
	DDLDropIndex
	DDLOther
)

// ParsedQuery holds the analysis result of a SQL statement.
type ParsedQuery struct {
	Original string
	Type     QueryType
	Tables   []TableRef
	DDLType  DDLType

	// For INSERT: target table columns
	TargetColumns []string

	// Raw parse tree for rewriting
	tree *pg_query.ParseResult
}

// IsReadOnly returns true for SELECT queries.
func (p *ParsedQuery) IsReadOnly() bool {
	return p.Type == QuerySelect
}

// IsWrite returns true for INSERT/UPDATE/DELETE.
func (p *ParsedQuery) IsWrite() bool {
	return p.Type == QueryInsert || p.Type == QueryUpdate || p.Type == QueryDelete
}

// IsDDL returns true for DDL statements.
func (p *ParsedQuery) IsDDL() bool {
	return p.Type == QueryDDL
}

// IsUtility returns true for utility statements (SET, SHOW, BEGIN, etc.).
func (p *ParsedQuery) IsUtility() bool {
	return p.Type == QueryUtility
}

// Parse parses a SQL string and returns a ParsedQuery.
func Parse(sql string) (*ParsedQuery, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parse sql: %w", err)
	}

	pq := &ParsedQuery{
		Original: sql,
		tree:     tree,
	}

	if len(tree.Stmts) == 0 {
		return pq, nil
	}

	// Analyze the first statement
	stmt := tree.Stmts[0].Stmt
	if stmt == nil {
		return pq, nil
	}

	classifyStatement(pq, stmt)

	return pq, nil
}

func classifyStatement(pq *ParsedQuery, stmt *pg_query.Node) {
	switch n := stmt.Node.(type) {
	case *pg_query.Node_SelectStmt:
		pq.Type = QuerySelect
		extractSelectTables(pq, n.SelectStmt)

	case *pg_query.Node_InsertStmt:
		pq.Type = QueryInsert
		extractInsertInfo(pq, n.InsertStmt)

	case *pg_query.Node_UpdateStmt:
		pq.Type = QueryUpdate
		extractUpdateTables(pq, n.UpdateStmt)

	case *pg_query.Node_DeleteStmt:
		pq.Type = QueryDelete
		extractDeleteTables(pq, n.DeleteStmt)

	case *pg_query.Node_CreateStmt:
		pq.Type = QueryDDL
		pq.DDLType = DDLCreateTable
		extractCreateTableRef(pq, n.CreateStmt)

	case *pg_query.Node_AlterTableStmt:
		pq.Type = QueryDDL
		pq.DDLType = DDLAlterTable
		extractRangeVarTable(pq, n.AlterTableStmt.Relation)

	case *pg_query.Node_DropStmt:
		pq.Type = QueryDDL
		classifyDropStmt(pq, n.DropStmt)

	case *pg_query.Node_IndexStmt:
		pq.Type = QueryDDL
		pq.DDLType = DDLCreateIndex
		extractRangeVarTable(pq, n.IndexStmt.Relation)

	case *pg_query.Node_TransactionStmt:
		pq.Type = QueryUtility

	case *pg_query.Node_VariableSetStmt:
		pq.Type = QueryUtility

	case *pg_query.Node_VariableShowStmt:
		pq.Type = QueryUtility

	default:
		// COPY, EXPLAIN, VACUUM, etc.
		pq.Type = QueryUtility
	}
}

func extractSelectTables(pq *ParsedQuery, sel *pg_query.SelectStmt) {
	if sel == nil {
		return
	}
	for _, from := range sel.FromClause {
		extractTableFromNode(pq, from)
	}
}

func extractInsertInfo(pq *ParsedQuery, ins *pg_query.InsertStmt) {
	if ins == nil {
		return
	}
	extractRangeVarTable(pq, ins.Relation)
	for _, col := range ins.Cols {
		if rt, ok := col.Node.(*pg_query.Node_ResTarget); ok {
			pq.TargetColumns = append(pq.TargetColumns, rt.ResTarget.Name)
		}
	}
}

func extractUpdateTables(pq *ParsedQuery, upd *pg_query.UpdateStmt) {
	if upd == nil {
		return
	}
	extractRangeVarTable(pq, upd.Relation)
	for _, from := range upd.FromClause {
		extractTableFromNode(pq, from)
	}
}

func extractDeleteTables(pq *ParsedQuery, del *pg_query.DeleteStmt) {
	if del == nil {
		return
	}
	extractRangeVarTable(pq, del.Relation)
}

func extractCreateTableRef(pq *ParsedQuery, cs *pg_query.CreateStmt) {
	if cs == nil {
		return
	}
	extractRangeVarTable(pq, cs.Relation)
}

func classifyDropStmt(pq *ParsedQuery, ds *pg_query.DropStmt) {
	switch ds.RemoveType {
	case pg_query.ObjectType_OBJECT_TABLE:
		pq.DDLType = DDLDropTable
	case pg_query.ObjectType_OBJECT_INDEX:
		pq.DDLType = DDLDropIndex
	default:
		pq.DDLType = DDLOther
	}
	// Extract table names from the objects list
	for _, obj := range ds.Objects {
		if list, ok := obj.Node.(*pg_query.Node_List); ok {
			ref, ok := extractTableRefFromList(list.List)
			if ok {
				pq.Tables = append(pq.Tables, ref)
			}
		}
	}
}

func extractTableRefFromList(list *pg_query.List) (TableRef, bool) {
	var parts []string
	for _, item := range list.Items {
		if s, ok := item.Node.(*pg_query.Node_String_); ok {
			parts = append(parts, s.String_.Sval)
		}
	}
	if len(parts) == 0 {
		return TableRef{}, false
	}
	ref := TableRef{Name: parts[len(parts)-1]}
	if len(parts) >= 2 {
		ref.Schema = parts[len(parts)-2]
	}
	return ref, true
}

func extractTableFromNode(pq *ParsedQuery, node *pg_query.Node) {
	if node == nil {
		return
	}
	switch n := node.Node.(type) {
	case *pg_query.Node_RangeVar:
		extractRangeVarTable(pq, n.RangeVar)
	case *pg_query.Node_JoinExpr:
		extractTableFromNode(pq, n.JoinExpr.Larg)
		extractTableFromNode(pq, n.JoinExpr.Rarg)
	case *pg_query.Node_RangeSubselect:
		// Subselects don't add to table list directly
	}
}

func extractRangeVarTable(pq *ParsedQuery, rv *pg_query.RangeVar) {
	if rv == nil {
		return
	}
	ref := TableRef{
		Name:  rv.Relname,
		Alias: "",
	}
	if rv.Schemaname != "" {
		ref.Schema = rv.Schemaname
	}
	if rv.Alias != nil {
		ref.Alias = rv.Alias.Aliasname
	}
	pq.Tables = append(pq.Tables, ref)
}

// IsTransactionControl returns true if sql is BEGIN/COMMIT/ROLLBACK/SAVEPOINT.
func IsTransactionControl(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") ||
		strings.HasPrefix(upper, "ROLLBACK") ||
		strings.HasPrefix(upper, "SAVEPOINT") ||
		strings.HasPrefix(upper, "RELEASE SAVEPOINT") ||
		strings.HasPrefix(upper, "START TRANSACTION") ||
		strings.HasPrefix(upper, "END")
}
