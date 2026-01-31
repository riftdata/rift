package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"
)

// Build-time variables (set via ldflags)
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "pgbranch",
	Short: "Instant, copy-on-write database branches for Postgres",
	Long: `pgbranch creates instant database branches using copy-on-write.
A 500GB production database branches in milliseconds, storing only the rows you change.

Documentation: https://pgbranch.dev/docs
GitHub: https://github.com/YOUR_USERNAME/pgbranch`,
	SilenceUsage:  true,
	SilenceErrors: true,
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pgbranch %s\n", version)
		fmt.Printf("  Commit:     %s\n", commit)
		fmt.Printf("  Built:      %s\n", buildTime)
		fmt.Printf("  Go version: %s\n", runtime.Version())
		fmt.Printf("  OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
	},
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize pgbranch with an upstream database",
	Long: `Initialize pgbranch by connecting to an upstream PostgreSQL database.
This creates the necessary metadata and prepares pgbranch for branching.`,
	Example: `  pgbranch init --upstream postgres://user:pass@localhost:5432/mydb
  pgbranch init --upstream postgres://localhost/mydb --data-dir /var/lib/pgbranch`,
	RunE: runInit,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the pgbranch proxy server",
	Long: `Start the pgbranch proxy server. This accepts PostgreSQL connections
and routes them to the appropriate branch.`,
	Example: `  pgbranch serve
  pgbranch serve --listen :6432 --api :8080
  pgbranch serve --config /etc/pgbranch/config.yaml`,
	RunE: runServe,
}

var createCmd = &cobra.Command{
	Use:   "create <branch-name>",
	Short: "Create a new branch",
	Long: `Create a new branch from the current main branch or a specified parent.
The branch is created instantly using copy-on-write.`,
	Example: `  pgbranch create feature-auth
  pgbranch create feature-auth --parent staging
  pgbranch create pr-123 --ttl 24h`,
	Args: cobra.ExactArgs(1),
	RunE: runCreate,
}

var deleteCmd = &cobra.Command{
	Use:     "delete <branch-name>",
	Aliases: []string{"rm", "remove"},
	Short:   "Delete a branch",
	Long:    `Delete a branch and free its storage. This cannot be undone.`,
	Example: `  pgbranch delete feature-auth
  pgbranch delete pr-123 --force`,
	Args: cobra.ExactArgs(1),
	RunE: runDelete,
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all branches",
	Long:    `List all branches with their status, parent, and storage usage.`,
	Example: `  pgbranch list
  pgbranch list --format json
  pgbranch list --all`,
	RunE: runList,
}

var statusCmd = &cobra.Command{
	Use:   "status [branch-name]",
	Short: "Show branch status",
	Long:  `Show detailed status of a branch or the overall system.`,
	Example: `  pgbranch status
  pgbranch status feature-auth`,
	RunE: runStatus,
}

var diffCmd = &cobra.Command{
	Use:   "diff <branch1> [branch2]",
	Short: "Show differences between branches",
	Long: `Show schema and data differences between two branches.
If branch2 is omitted, compares against main.`,
	Example: `  pgbranch diff feature-auth
  pgbranch diff feature-auth staging
  pgbranch diff feature-auth --schema-only`,
	Args: cobra.RangeArgs(1, 2),
	RunE: runDiff,
}

var mergeCmd = &cobra.Command{
	Use:   "merge <branch-name>",
	Short: "Generate merge SQL for a branch",
	Long: `Generate SQL statements to merge a branch's changes into its parent.
This does not execute the SQL, only outputs it.`,
	Example: `  pgbranch merge feature-auth
  pgbranch merge feature-auth --dry-run
  pgbranch merge feature-auth > migration.sql`,
	Args: cobra.ExactArgs(1),
	RunE: runMerge,
}

var connectCmd = &cobra.Command{
	Use:   "connect <branch-name>",
	Short: "Connect to a branch using psql",
	Long:  `Open an interactive psql session connected to the specified branch.`,
	Example: `  pgbranch connect feature-auth
  pgbranch connect main`,
	Args: cobra.ExactArgs(1),
	RunE: runConnect,
}

// Global flags
var (
	configFile string
	dataDir    string
	logLevel   string
	logFormat  string
)

// init command flags
var (
	upstreamURL string
)

// serve command flags
var (
	listenAddr string
	apiAddr    string
)

// create command flags
var (
	parentBranch string
	branchTTL    string
)

// delete command flags
var (
	forceDelete bool
)

// list command flags
var (
	outputFormat string
	showAll      bool
)

// diff command flags
var (
	schemaOnly bool
	dataOnly   bool
)

// merge command flags
var (
	dryRun bool
)

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "config file (default: $HOME/.pgbranch/config.yaml)")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "", "data directory (default: $HOME/.pgbranch)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "log format (text, json)")

	// init command flags
	initCmd.Flags().StringVar(&upstreamURL, "upstream", "", "upstream PostgreSQL connection URL (required)")
	err := initCmd.MarkFlagRequired("upstream")
	if err != nil {
		return
	}

	// serve command flags
	serveCmd.Flags().StringVar(&listenAddr, "listen", ":6432", "proxy listen address")
	serveCmd.Flags().StringVar(&apiAddr, "api", ":8080", "API/dashboard listen address")

	// create command flags
	createCmd.Flags().StringVar(&parentBranch, "parent", "main", "parent branch")
	createCmd.Flags().StringVar(&branchTTL, "ttl", "", "auto-delete after duration (e.g., 24h, 7d)")

	// delete command flags
	deleteCmd.Flags().BoolVarP(&forceDelete, "force", "f", false, "force delete without confirmation")

	// list command flags
	listCmd.Flags().StringVarP(&outputFormat, "format", "o", "table", "output format (table, json, yaml)")
	listCmd.Flags().BoolVarP(&showAll, "all", "a", false, "show all branches including deleted")

	// diff command flags
	diffCmd.Flags().BoolVar(&schemaOnly, "schema-only", false, "show only schema differences")
	diffCmd.Flags().BoolVar(&dataOnly, "data-only", false, "show only data differences")

	// merge command flags
	mergeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show SQL without executing")

	// Add commands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(mergeCmd)
	rootCmd.AddCommand(connectCmd)
}

// Command implementations (stubs for now)

func runInit(cmd *cobra.Command, args []string) error {
	fmt.Printf("Initializing pgbranch with upstream: %s\n", upstreamURL)
	fmt.Printf("Data directory: %s\n", dataDir)
	// TODO: Implement
	return nil
}

func runServe(cmd *cobra.Command, args []string) error {
	fmt.Printf("Starting pgbranch proxy...\n")
	fmt.Printf("  Proxy:     %s\n", listenAddr)
	fmt.Printf("  API:       %s\n", apiAddr)
	fmt.Printf("  Log level: %s\n", logLevel)
	// TODO: Implement
	select {} // Block forever for now
}

func runCreate(cmd *cobra.Command, args []string) error {
	branchName := args[0]
	fmt.Printf("Creating branch '%s' from '%s'\n", branchName, parentBranch)
	if branchTTL != "" {
		fmt.Printf("  TTL: %s\n", branchTTL)
	}
	// TODO: Implement
	return nil
}

func runDelete(cmd *cobra.Command, args []string) error {
	branchName := args[0]
	fmt.Printf("Deleting branch '%s'\n", branchName)
	// TODO: Implement
	return nil
}

func runList(cmd *cobra.Command, args []string) error {
	fmt.Println("Branches:")
	fmt.Println("  main (default)")
	// TODO: Implement
	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		fmt.Printf("Status of branch '%s'\n", args[0])
	} else {
		fmt.Println("pgbranch status")
	}
	// TODO: Implement
	return nil
}

func runDiff(cmd *cobra.Command, args []string) error {
	branch1 := args[0]
	branch2 := "main"
	if len(args) > 1 {
		branch2 = args[1]
	}
	fmt.Printf("Diff between '%s' and '%s'\n", branch1, branch2)
	// TODO: Implement
	return nil
}

func runMerge(cmd *cobra.Command, args []string) error {
	branchName := args[0]
	fmt.Printf("Generating merge SQL for '%s'\n", branchName)
	if dryRun {
		fmt.Println("(dry run)")
	}
	// TODO: Implement
	return nil
}

func runConnect(cmd *cobra.Command, args []string) error {
	branchName := args[0]
	fmt.Printf("Connecting to branch '%s'...\n", branchName)
	// TODO: Implement - exec psql
	return nil
}
