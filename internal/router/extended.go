package router

import (
	"context"
	"fmt"
	"strings"

	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/parser"
	"github.com/riftdata/rift/internal/pgwire"
)

// preparedStmt holds a parsed statement waiting for binding.
type preparedStmt struct {
	name      string
	sql       string
	processed *cow.ProcessedQuery
}

// portal holds a bound statement ready for execution.
type portal struct {
	name      string
	stmt      *preparedStmt
	paramVals [][]byte
}

// extendedState tracks Parse/Bind/Execute state per session.
type extendedState struct {
	stmts   map[string]*preparedStmt // name -> prepared statement
	portals map[string]*portal       // name -> portal
}

func newExtendedState() *extendedState {
	return &extendedState{
		stmts:   make(map[string]*preparedStmt),
		portals: make(map[string]*portal),
	}
}

// handleParse processes a Parse ('P') message.
// Format: name(string) query(string) numParamTypes(int16) paramTypes(int32[]...)
func (s *Session) handleParse(ctx context.Context, payload []byte) error {
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload)
	buf.SetPosition(0)

	name, err := buf.ReadString()
	if err != nil {
		return fmt.Errorf("read statement name: %w", err)
	}

	sql, err := buf.ReadString()
	if err != nil {
		return fmt.Errorf("read query: %w", err)
	}

	// We skip parameter type OIDs — we let PostgreSQL infer types
	// numParams, _ := buf.ReadInt16()

	// Process through CoW engine
	sql = strings.TrimSpace(sql)
	var processed *cow.ProcessedQuery

	switch {
	case sql == "":
		processed = &cow.ProcessedQuery{
			OriginalSQL:   "",
			RewrittenSQL:  "",
			IsPassthrough: true,
		}
	case isBegin(sql) || isCommit(sql) || isRollback(sql) || parser.IsTransactionControl(sql):
		processed = &cow.ProcessedQuery{
			OriginalSQL:   sql,
			RewrittenSQL:  sql,
			Type:          parser.QueryUtility,
			IsPassthrough: true,
		}
	default:
		processed, err = s.engine.ProcessQuery(ctx, s.branchName, sql)
		if err != nil {
			s.extErr = fmt.Errorf("parse query: %w", err)
			// Don't send error yet — wait for Sync
			return nil
		}
	}

	stmt := &preparedStmt{
		name:      name,
		sql:       sql,
		processed: processed,
	}

	s.ext.stmts[name] = stmt

	// Send ParseComplete
	return s.client.WriteMessage(pgwire.MsgParseComplete, nil)
}

// handleBind processes a Bind ('B') message.
// Format: portal(string) statement(string) numFormats(int16) formats(int16[])
//
//	numParams(int16) paramValues(int32 len + bytes[]) numResultFormats(int16) resultFormats(int16[])
func (s *Session) handleBind(_ context.Context, payload []byte) error {
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload)
	buf.SetPosition(0)

	portalName, err := buf.ReadString()
	if err != nil {
		return fmt.Errorf("read portal name: %w", err)
	}

	stmtName, err := buf.ReadString()
	if err != nil {
		return fmt.Errorf("read statement name: %w", err)
	}

	stmt, ok := s.ext.stmts[stmtName]
	if !ok {
		s.extErr = fmt.Errorf("statement %q not found", stmtName)
		return nil
	}

	// Read parameter format codes
	numFormats, err := buf.ReadInt16()
	if err != nil {
		return fmt.Errorf("read num formats: %w", err)
	}
	for i := int16(0); i < numFormats; i++ {
		_, _ = buf.ReadInt16() // skip format codes — we use text
	}

	// Read parameter values
	numParams, err := buf.ReadInt16()
	if err != nil {
		return fmt.Errorf("read num params: %w", err)
	}

	paramVals := make([][]byte, numParams)
	for i := int16(0); i < numParams; i++ {
		length, err := buf.ReadInt32()
		if err != nil {
			return fmt.Errorf("read param length: %w", err)
		}
		if length == -1 {
			paramVals[i] = nil // NULL
		} else {
			val, err := buf.ReadBytes(int(length))
			if err != nil {
				return fmt.Errorf("read param value: %w", err)
			}
			paramVals[i] = val
		}
	}

	// Skip result format codes — we always return text
	// numResultFormats, _ := buf.ReadInt16()

	p := &portal{
		name:      portalName,
		stmt:      stmt,
		paramVals: paramVals,
	}
	s.ext.portals[portalName] = p

	// Send BindComplete
	return s.client.WriteMessage(pgwire.MsgBindComplete, nil)
}

// handleDescribe processes a Describe ('D') message.
// Format: type(byte: 'S' or 'P') name(string)
func (s *Session) handleDescribe(_ context.Context, payload []byte) error {
	if len(payload) < 2 {
		s.extErr = fmt.Errorf("invalid describe message")
		return nil
	}

	descType := payload[0]
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload[1:])
	buf.SetPosition(0)
	name, _ := buf.ReadString()

	switch descType {
	case 'S':
		// Describe statement — send ParameterDescription + NoData/RowDescription
		_, ok := s.ext.stmts[name]
		if !ok {
			s.extErr = fmt.Errorf("statement %q not found", name)
			return nil
		}
		// Send empty ParameterDescription (no params described)
		paramBuf := pgwire.NewBuffer(4)
		paramBuf.WriteInt16(0) // zero parameters
		if err := s.client.WriteMessage(pgwire.MsgParameterDescription, paramBuf.Bytes()); err != nil {
			return err
		}
		// Send NoData — we don't know the row description without executing
		return s.client.WriteMessage(pgwire.MsgNoData, nil)

	case 'P':
		// Describe portal
		_, ok := s.ext.portals[name]
		if !ok {
			s.extErr = fmt.Errorf("portal %q not found", name)
			return nil
		}
		// Send NoData — we don't know the row description without executing
		return s.client.WriteMessage(pgwire.MsgNoData, nil)

	default:
		s.extErr = fmt.Errorf("invalid describe type: %c", descType)
		return nil
	}
}

// handleExecute processes an Execute ('E') message.
// Format: portal(string) maxRows(int32)
func (s *Session) handleExecute(ctx context.Context, payload []byte) error {
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload)
	buf.SetPosition(0)

	portalName, err := buf.ReadString()
	if err != nil {
		return fmt.Errorf("read portal name: %w", err)
	}

	maxRows, err := buf.ReadInt32()
	if err != nil {
		return fmt.Errorf("read max rows: %w", err)
	}
	_ = maxRows // We don't support partial execution yet

	p, ok := s.ext.portals[portalName]
	if !ok {
		s.extErr = fmt.Errorf("portal %q not found", portalName)
		return nil
	}

	processed := p.stmt.processed
	if processed == nil {
		s.extErr = fmt.Errorf("statement not processed")
		return nil
	}

	sql := processed.RewrittenSQL
	if sql == "" {
		// Empty query
		return pgwire.WriteMessage(s.client.NetConn(), pgwire.MsgEmptyQueryResponse, nil)
	}

	// Handle transaction control
	if isBegin(p.stmt.sql) {
		return s.handleExtBegin(ctx)
	}
	if isCommit(p.stmt.sql) {
		return s.handleExtCommit(ctx)
	}
	if isRollback(p.stmt.sql) {
		return s.handleExtRollback(ctx)
	}

	// Convert [][]byte params to []interface{}
	args := make([]interface{}, len(p.paramVals))
	for i, v := range p.paramVals {
		if v == nil {
			args[i] = nil
		} else {
			args[i] = string(v)
		}
	}

	return s.executeExtStatements(ctx, processed, sql, args)
}

// executeExtStatements runs the statements for an extended protocol Execute.
func (s *Session) executeExtStatements(ctx context.Context, processed *cow.ProcessedQuery, sql string, args []interface{}) error {
	statements := splitStatements(sql)
	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		isLast := i == len(statements)-1
		if err := s.executeExtOne(ctx, processed, stmt, isLast, args); err != nil {
			return err
		}
		args = nil // only the first statement gets params
	}
	return nil
}

// executeExtOne runs a single statement within the extended protocol.
func (s *Session) executeExtOne(ctx context.Context, processed *cow.ProcessedQuery, stmt string, isLast bool, args []interface{}) error {
	if processed.Type == parser.QuerySelect && isLast {
		rows, err := s.query(ctx, stmt, args...)
		if err != nil {
			if s.txStatus == pgwire.TxStatusInTx {
				s.txStatus = pgwire.TxStatusFailed
			}
			s.extErr = err
			return nil
		}
		return sendQueryResult(s.client, rows, "")
	}

	tag, err := s.runExec(ctx, stmt, args...)
	if err != nil {
		if s.txStatus == pgwire.TxStatusInTx {
			s.txStatus = pgwire.TxStatusFailed
		}
		s.extErr = err
		return nil
	}
	if isLast {
		return s.client.SendCommandComplete(tag)
	}
	return nil
}

// handleClose processes a Close ('C') message.
// Format: type(byte: 'S' or 'P') name(string)
func (s *Session) handleClose(_ context.Context, payload []byte) error {
	if len(payload) < 2 {
		return s.client.WriteMessage(pgwire.MsgCloseComplete, nil)
	}

	closeType := payload[0]
	buf := pgwire.NewBuffer(0)
	buf.WriteBytes(payload[1:])
	buf.SetPosition(0)
	name, _ := buf.ReadString()

	switch closeType {
	case 'S':
		delete(s.ext.stmts, name)
	case 'P':
		delete(s.ext.portals, name)
	}

	return s.client.WriteMessage(pgwire.MsgCloseComplete, nil)
}

// handleSync processes a Sync ('S') message — ends the extended query cycle.
func (s *Session) handleSync() error {
	if s.extErr != nil {
		_ = s.client.SendError("ERROR", pgwire.ErrCodeInternalError, s.extErr.Error())
		s.extErr = nil
	}
	return s.client.SendReadyForQuery(s.txStatus)
}

// Transaction helpers for extended protocol (no ReadyForQuery — Sync does that)

func (s *Session) handleExtBegin(ctx context.Context) error {
	if s.tx != nil {
		return s.client.SendCommandComplete("BEGIN")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		s.extErr = err
		return nil
	}
	s.tx = tx
	s.txStatus = pgwire.TxStatusInTx
	return s.client.SendCommandComplete("BEGIN")
}

func (s *Session) handleExtCommit(ctx context.Context) error {
	if s.tx == nil {
		return s.client.SendCommandComplete("COMMIT")
	}
	err := s.tx.Commit(ctx)
	s.tx = nil
	s.txStatus = pgwire.TxStatusIdle
	if err != nil {
		s.extErr = err
		return nil
	}
	return s.client.SendCommandComplete("COMMIT")
}

func (s *Session) handleExtRollback(ctx context.Context) error {
	if s.tx == nil {
		return s.client.SendCommandComplete("ROLLBACK")
	}
	err := s.tx.Rollback(ctx)
	s.tx = nil
	s.txStatus = pgwire.TxStatusIdle
	if err != nil {
		s.extErr = err
		return nil
	}
	return s.client.SendCommandComplete("ROLLBACK")
}
