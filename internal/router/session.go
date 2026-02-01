package router

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/parser"
	"github.com/riftdata/rift/internal/pgwire"
)

// Session handles query processing for a single client connection on a non-main branch.
type Session struct {
	client     *pgwire.ClientConn
	pool       *pgxpool.Pool
	engine     *cow.Engine
	branchName string

	// Transaction state
	tx       pgx.Tx
	txStatus byte // 'I', 'T', or 'E'

	// Extended query protocol state
	ext    *extendedState
	extErr error // deferred error until Sync
}

// NewSession creates a new session for a branch connection.
func NewSession(client *pgwire.ClientConn, pool *pgxpool.Pool, engine *cow.Engine, branchName string) *Session {
	return &Session{
		client:     client,
		pool:       pool,
		engine:     engine,
		branchName: branchName,
		txStatus:   pgwire.TxStatusIdle,
		ext:        newExtendedState(),
	}
}

// HandleMessages processes messages from the client until the connection closes.
func (s *Session) HandleMessages(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msgType, payload, err := s.client.ReadMessage()
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		if msgType == pgwire.MsgTerminate {
			return nil
		}

		if err := s.dispatchMessage(ctx, msgType, payload); err != nil {
			return err
		}
	}
}

// dispatchMessage routes a single wire protocol message to its handler.
func (s *Session) dispatchMessage(ctx context.Context, msgType byte, payload []byte) error {
	switch msgType {
	case pgwire.MsgQuery:
		return wrapErr("handle query", s.handleSimpleQuery(ctx, payload))
	case pgwire.MsgParse:
		return wrapErr("handle parse", s.handleParse(ctx, payload))
	case pgwire.MsgBind:
		return wrapErr("handle bind", s.handleBind(ctx, payload))
	case pgwire.MsgDescribe:
		return wrapErr("handle describe", s.handleDescribe(ctx, payload))
	case pgwire.MsgExecute:
		return wrapErr("handle execute", s.handleExecute(ctx, payload))
	case pgwire.MsgClose:
		return wrapErr("handle close", s.handleClose(ctx, payload))
	case pgwire.MsgSync:
		return wrapErr("handle sync", s.handleSync())
	case pgwire.MsgFlush:
		return nil // Flush is a no-op — we write immediately
	default:
		return s.client.SendReadyForQuery(s.txStatus)
	}
}

func wrapErr(label string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

// handleSimpleQuery processes a 'Q' message containing one or more SQL statements.
func (s *Session) handleSimpleQuery(ctx context.Context, payload []byte) error {
	// Extract SQL (null-terminated string)
	sql := strings.TrimSuffix(string(payload), "\x00")
	sql = strings.TrimSpace(sql)

	if sql == "" {
		// Empty query
		if err := pgwire.WriteMessage(s.client.NetConn(), pgwire.MsgEmptyQueryResponse, nil); err != nil {
			return err
		}
		return s.client.SendReadyForQuery(s.txStatus)
	}

	// Handle transaction control
	if isBegin(sql) {
		return s.handleBegin(ctx)
	}
	if isCommit(sql) {
		return s.handleCommit(ctx)
	}
	if isRollback(sql) {
		return s.handleRollback(ctx)
	}

	// Process through the CoW engine
	processed, err := s.engine.ProcessQuery(ctx, s.branchName, sql)
	if err != nil {
		return s.sendQueryError(err)
	}

	// Execute the query
	if err := s.executeProcessed(ctx, processed); err != nil {
		return s.sendQueryError(err)
	}

	return s.client.SendReadyForQuery(s.txStatus)
}

// executeProcessed runs a processed query and sends results to the client.
func (s *Session) executeProcessed(ctx context.Context, pq *cow.ProcessedQuery) error {
	sqlToRun := pq.RewrittenSQL

	// For multi-statement rewrites (UPDATE/DELETE with copy-on-write),
	// split on semicolons and run each
	statements := splitStatements(sqlToRun)

	for i, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		isLast := i == len(statements)-1

		// Determine if this is a query (returns rows) or statement
		if pq.Type == parser.QuerySelect && isLast {
			rows, err := s.query(ctx, stmt)
			if err != nil {
				if s.txStatus == pgwire.TxStatusInTx {
					s.txStatus = pgwire.TxStatusFailed
				}
				return err
			}
			if err := sendQueryResult(s.client, rows, ""); err != nil {
				return err
			}
		} else {
			tag, err := s.runExec(ctx, stmt)
			if err != nil {
				if s.txStatus == pgwire.TxStatusInTx {
					s.txStatus = pgwire.TxStatusFailed
				}
				return err
			}
			if isLast {
				if err := s.client.SendCommandComplete(tag); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// query runs a SQL query and returns rows.
func (s *Session) query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	if s.tx != nil {
		return s.tx.Query(ctx, sql, args...)
	}
	return s.pool.Query(ctx, sql, args...)
}

// runExec runs a SQL statement that doesn't return rows.
func (s *Session) runExec(ctx context.Context, sql string, args ...interface{}) (string, error) {
	if s.tx != nil {
		tag, err := s.tx.Exec(ctx, sql, args...)
		return tag.String(), err
	}
	tag, err := s.pool.Exec(ctx, sql, args...)
	return tag.String(), err
}

func (s *Session) handleBegin(ctx context.Context) error {
	if s.tx != nil {
		// Already in a transaction
		if err := s.client.SendCommandComplete("BEGIN"); err != nil {
			return err
		}
		return s.client.SendReadyForQuery(s.txStatus)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return s.sendQueryError(err)
	}
	s.tx = tx
	s.txStatus = pgwire.TxStatusInTx

	if err := s.client.SendCommandComplete("BEGIN"); err != nil {
		return err
	}
	return s.client.SendReadyForQuery(s.txStatus)
}

func (s *Session) handleCommit(ctx context.Context) error {
	if s.tx == nil {
		// Not in a transaction — Postgres sends a warning but succeeds
		if err := s.client.SendCommandComplete("COMMIT"); err != nil {
			return err
		}
		return s.client.SendReadyForQuery(s.txStatus)
	}

	err := s.tx.Commit(ctx)
	s.tx = nil
	s.txStatus = pgwire.TxStatusIdle

	if err != nil {
		return s.sendQueryError(err)
	}

	if err := s.client.SendCommandComplete("COMMIT"); err != nil {
		return err
	}
	return s.client.SendReadyForQuery(s.txStatus)
}

func (s *Session) handleRollback(ctx context.Context) error {
	if s.tx == nil {
		if err := s.client.SendCommandComplete("ROLLBACK"); err != nil {
			return err
		}
		return s.client.SendReadyForQuery(s.txStatus)
	}

	err := s.tx.Rollback(ctx)
	s.tx = nil
	s.txStatus = pgwire.TxStatusIdle

	if err != nil {
		return s.sendQueryError(err)
	}

	if err := s.client.SendCommandComplete("ROLLBACK"); err != nil {
		return err
	}
	return s.client.SendReadyForQuery(s.txStatus)
}

func (s *Session) sendQueryError(err error) error {
	_ = s.client.SendError("ERROR", pgwire.ErrCodeInternalError, err.Error())
	return s.client.SendReadyForQuery(s.txStatus)
}

// Cleanup releases session resources.
func (s *Session) Cleanup(ctx context.Context) {
	if s.tx != nil {
		_ = s.tx.Rollback(ctx)
		s.tx = nil
	}
}

func isBegin(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return upper == "BEGIN" || strings.HasPrefix(upper, "BEGIN;") ||
		upper == "START TRANSACTION" || strings.HasPrefix(upper, "START TRANSACTION;")
}

func isCommit(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return upper == "COMMIT" || strings.HasPrefix(upper, "COMMIT;") ||
		upper == "END" || strings.HasPrefix(upper, "END;")
}

func isRollback(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return upper == "ROLLBACK" || strings.HasPrefix(upper, "ROLLBACK;")
}

// splitStatements splits SQL on semicolons, respecting basic quoting.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inSingle := false
	inDouble := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if c == '\'' && !inDouble {
			inSingle = !inSingle
		} else if c == '"' && !inSingle {
			inDouble = !inDouble
		}

		if c == ';' && !inSingle && !inDouble {
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	s := strings.TrimSpace(current.String())
	if s != "" {
		stmts = append(stmts, s)
	}

	return stmts
}
