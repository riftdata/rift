package router

import (
	"context"
	"net"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/pgwire"
)

// Router handles query routing for branch connections.
// Main branch connections bypass the router entirely (raw TCP passthrough).
// Non-main branch connections are handled via the CoW engine.
type Router struct {
	pool   *pgxpool.Pool
	engine *cow.Engine
}

// New creates a new Router.
func New(pool *pgxpool.Pool, engine *cow.Engine) *Router {
	return &Router{
		pool:   pool,
		engine: engine,
	}
}

// HandleSession handles a client connection for a non-main branch.
// This takes over from the proxy after handshake and branch resolution.
// The upstream TCP connection is not used — queries go through pgx pool instead.
func (r *Router) HandleSession(ctx context.Context, client *pgwire.ClientConn, branchName string) error {
	session := NewSession(client, r.pool, r.engine, branchName)
	defer session.Cleanup(ctx)

	return session.HandleMessages(ctx)
}

// IsBranchRouted returns true if a branch should go through the CoW router
// rather than raw TCP passthrough.
func IsBranchRouted(branchName string) bool {
	return branchName != "main" && branchName != ""
}

// IsPassthroughBranch returns true if a branch should use raw TCP passthrough.
func IsPassthroughBranch(branchName string) bool {
	return branchName == "main" || branchName == ""
}

// CloseUpstream closes an upstream TCP connection that is no longer needed
// (because the session is handled by the router instead).
func CloseUpstream(upstream net.Conn) {
	if upstream != nil {
		_ = upstream.Close()
	}
}
