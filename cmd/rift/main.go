package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/riftdata/rift/internal/config"
	"github.com/riftdata/rift/internal/cow"
	"github.com/riftdata/rift/internal/server"
	"github.com/riftdata/rift/internal/storage"
	"github.com/riftdata/rift/internal/ui"
)

// Build-time variables
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

// Global flags
var (
	cfgFile string
	noColor bool
	quiet   bool
	verbose bool
	output  string
)

// Global instances
var (
	cfg *config.Config
	out *ui.Output
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if out != nil {
			out.Error(err.Error())
		} else {
			_, err := fmt.Fprintln(os.Stderr, err)
			if err != nil {
				return 1
			}
		}
		return 1
	}
	return 0
}

var rootCmd = &cobra.Command{
	Use:   "rift",
	Short: "Instant, copy-on-write database branches for Postgres",
	Long: `rift creates instant database branches using copy-on-write.
A 500GB production database branches in milliseconds, storing only the rows you change.

Get started:
  rift init --upstream postgres://localhost:5432/mydb
  rift serve
  rift create my-feature-branch

Documentation: https://riftdata.io/docs`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Skip for completion and help commands
		if cmd.Name() == "completion" || cmd.Name() == "help" {
			return nil
		}

		// Initialize output
		format := ui.OutputFormat(output)
		out = ui.NewOutput(format, noColor, quiet)

		// Load config (don't fail if not found for init command)
		var err error
		cfg, err = config.Load(cfgFile)
		if err != nil && cmd.Name() != "init" {
			return fmt.Errorf("loading config: %w", err)
		}

		return nil
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		if output == "json" {
			err := out.JSON(map[string]string{
				"version":   version,
				"commit":    commit,
				"buildTime": buildTime,
				"goVersion": runtime.Version(),
				"os":        runtime.GOOS,
				"arch":      runtime.GOARCH,
			})
			if err != nil {
				return
			}
			return
		}

		out.Title("rift")
		out.KeyValue("Version", version)
		out.KeyValue("Commit", commit)
		out.KeyValue("Built", buildTime)
		out.KeyValue("Go", runtime.Version())
		out.KeyValue("OS/Arch", fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH))
	},
}

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion scripts",
	Long: `Generate shell completion scripts for rift.

To load completions:

Bash:
  $ source <(rift completion bash)
  # To load completions for each session, execute once:
  # Linux:
  $ rift completion bash > /etc/bash_completion.d/rift
  # macOS:
  $ rift completion bash > $(brew --prefix)/etc/bash_completion.d/rift

Zsh:
  # If shell completion is not already enabled in your environment,
  # you will need to enable it. You can execute the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
  
  # To load completions for each session, execute once:
  $ rift completion zsh > "${fpath[1]}/_rift"
  
  # You will need to start a new shell for this setup to take effect.

Fish:
  $ rift completion fish | source
  # To load completions for each session, execute once:
  $ rift completion fish > ~/.config/fish/completions/rift.fish

PowerShell:
  PS> rift completion powershell | Out-String | Invoke-Expression
  # To load completions for every new session, run:
  PS> rift completion powershell > rift.ps1
  # and source this file from your PowerShell profile.
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		switch args[0] {
		case "bash":
			err := cmd.Root().GenBashCompletion(os.Stdout)
			if err != nil {
				return
			}
		case "zsh":
			err := cmd.Root().GenZshCompletion(os.Stdout)
			if err != nil {
				return
			}
		case "fish":
			err := cmd.Root().GenFishCompletion(os.Stdout, true)
			if err != nil {
				return
			}
		case "powershell":
			err := cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			if err != nil {
				return
			}
		}
	},
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize rift with an upstream database",
	Long: `Initialize rift by connecting to an upstream PostgreSQL database.
This creates the necessary metadata and prepares rift for branching.

If --upstream is not provided, an interactive prompt will guide you through setup.`,
	Example: `  # Interactive setup
  rift init

  # With connection URL
  rift init --upstream postgres://user:pass@localhost:5432/mydb

  # With custom data directory
  rift init --upstream postgres://localhost/mydb --data-dir /var/lib/rift`,
	RunE: runInit,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the rift proxy server",
	Long: `Start the rift proxy server. This accepts PostgreSQL connections
and routes them to the appropriate branch.

The proxy listens for Postgres connections on --listen (default :6432) and
serves the HTTP API/dashboard on --api (default :8080).`,
	Example: `  rift serve
  rift serve --listen :6432 --api :8080
  rift serve --config /etc/rift/config.yaml`,
	RunE: runServe,
}

var createCmd = &cobra.Command{
	Use:   "create [branch-name]",
	Short: "Create a new branch",
	Long: `Create a new branch from the current main branch or a specified parent.
The branch is created instantly using copy-on-write.

If branch-name is not provided, an interactive prompt will guide you.`,
	Example: `  # Interactive
  rift create

  # With name
  rift create feature-auth

  # From specific parent
  rift create feature-auth --parent staging

  # With auto-delete
  rift create pr-123 --ttl 24h`,
	Args: cobra.MaximumNArgs(1),
	RunE: runCreate,
	ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		// No completions for branch name - it's a new name
		return nil, cobra.ShellCompDirectiveNoFileComp
	},
}

var deleteCmd = &cobra.Command{
	Use:     "delete <branch-name>",
	Aliases: []string{"rm", "remove"},
	Short:   "Delete a branch",
	Long:    `Delete a branch and free its storage. This cannot be undone.`,
	Example: `  rift delete feature-auth
  rift delete pr-123 --force`,
	Args:              cobra.ExactArgs(1),
	RunE:              runDelete,
	ValidArgsFunction: completeBranches,
}

var listCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List all branches",
	Long:    `List all branches with their status, parent, and storage usage.`,
	Example: `  rift list
  rift list --format json
  rift list --all`,
	RunE: runList,
}

var statusCmd = &cobra.Command{
	Use:   "status [branch-name]",
	Short: "Show branch or system status",
	Long:  `Show detailed status of a branch or the overall system.`,
	Example: `  rift status
  rift status feature-auth`,
	Args:              cobra.MaximumNArgs(1),
	RunE:              runStatus,
	ValidArgsFunction: completeBranches,
}

var diffCmd = &cobra.Command{
	Use:   "diff <branch1> [branch2]",
	Short: "Show differences between branches",
	Long: `Show schema and data differences between two branches.
If branch2 is omitted, compares against main.`,
	Example: `  rift diff feature-auth
  rift diff feature-auth staging
  rift diff feature-auth --schema-only`,
	Args:              cobra.RangeArgs(1, 2),
	RunE:              runDiff,
	ValidArgsFunction: completeBranches,
}

var mergeCmd = &cobra.Command{
	Use:   "merge <branch-name>",
	Short: "Generate merge SQL for a branch",
	Long: `Generate SQL statements to merge a branch's changes into its parent.
This does not execute the SQL, only outputs it.`,
	Example: `  rift merge feature-auth
  rift merge feature-auth --dry-run
  rift merge feature-auth > migration.sql`,
	Args:              cobra.ExactArgs(1),
	RunE:              runMerge,
	ValidArgsFunction: completeBranches,
}

var connectCmd = &cobra.Command{
	Use:   "connect <branch-name>",
	Short: "Connect to a branch using psql",
	Long:  `Open an interactive psql session connected to the specified branch.`,
	Example: `  rift connect feature-auth
  rift connect main`,
	Args:              cobra.ExactArgs(1),
	RunE:              runConnect,
	ValidArgsFunction: completeBranches,
}

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage configuration",
	Long:  `View and manage rift configuration.`,
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	RunE:  runConfigShow,
}

var configSetCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Set a configuration value",
	Args:  cobra.ExactArgs(2),
	RunE:  runConfigSet,
}

var configPathCmd = &cobra.Command{
	Use:   "path",
	Short: "Show configuration file path",
	Run: func(cmd *cobra.Command, args []string) {
		if cfgFile != "" {
			fmt.Println(cfgFile)
		} else {
			fmt.Println(viper.ConfigFileUsed())
		}
	},
}

// Flag variables
var (
	upstreamURL  string
	dataDir      string
	listenAddr   string
	apiAddr      string
	parentBranch string
	branchTTL    string
	forceDelete  bool
	showAll      bool
	schemaOnly   bool
	dataOnly     bool
	dryRun       bool
	interactive  bool
)

func init() {
	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: $HOME/.rift/config.yaml)")
	rootCmd.PersistentFlags().BoolVar(&noColor, "no-color", false, "disable color output")
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "suppress non-essential output")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "table", "output format (table, json, yaml)")

	// init flags
	initCmd.Flags().StringVar(&upstreamURL, "upstream", "", "upstream PostgreSQL connection URL")
	initCmd.Flags().StringVar(&dataDir, "data-dir", "", "data directory (default: $HOME/.rift)")
	initCmd.Flags().BoolVarP(&interactive, "interactive", "i", false, "force interactive mode")

	// serve flags
	serveCmd.Flags().StringVar(&listenAddr, "listen", ":6432", "proxy listen address")
	serveCmd.Flags().StringVar(&apiAddr, "api", ":8080", "API/dashboard listen address")

	// create flags
	createCmd.Flags().StringVar(&parentBranch, "parent", "main", "parent branch")
	createCmd.Flags().StringVar(&branchTTL, "ttl", "", "auto-delete after duration (e.g., 24h, 7d)")
	createCmd.Flags().BoolVarP(&interactive, "interactive", "i", false, "force interactive mode")

	// delete flags
	deleteCmd.Flags().BoolVarP(&forceDelete, "force", "f", false, "skip confirmation")

	// list flags
	listCmd.Flags().BoolVarP(&showAll, "all", "a", false, "show all branches including deleted")

	// diff flags
	diffCmd.Flags().BoolVar(&schemaOnly, "schema-only", false, "show only schema differences")
	diffCmd.Flags().BoolVar(&dataOnly, "data-only", false, "show only data differences")

	// merge flags
	mergeCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show SQL without executing")

	// config subcommands
	configCmd.AddCommand(configShowCmd)
	configCmd.AddCommand(configSetCmd)
	configCmd.AddCommand(configPathCmd)

	// Add commands
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(completionCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(deleteCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(diffCmd)
	rootCmd.AddCommand(mergeCmd)
	rootCmd.AddCommand(connectCmd)
	rootCmd.AddCommand(configCmd)

	// Register completion functions
	err := rootCmd.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "json", "yaml"}, cobra.ShellCompDirectiveNoFileComp
	})
	if err != nil {
		return
	}

	err = createCmd.RegisterFlagCompletionFunc("parent", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		// TODO: Return actual branches
		return []string{"main"}, cobra.ShellCompDirectiveNoFileComp
	})
	if err != nil {
		return
	}

	err = createCmd.RegisterFlagCompletionFunc("ttl", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"1h", "6h", "24h", "7d", "30d"}, cobra.ShellCompDirectiveNoFileComp
	})
	if err != nil {
		return
	}
}

// Completion function for branch names
func completeBranches(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if cfg == nil || cfg.Upstream.URL == "" {
		return []string{"main"}, cobra.ShellCompDirectiveNoFileComp
	}
	store, err := storage.New(cmd.Context(), cfg.Upstream.URL)
	if err != nil {
		return []string{"main"}, cobra.ShellCompDirectiveNoFileComp
	}
	defer store.Close()

	branches, err := store.ListBranches(cmd.Context())
	if err != nil {
		return []string{"main"}, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, len(branches))
	for i, b := range branches {
		names[i] = b.Name
	}
	return names, cobra.ShellCompDirectiveNoFileComp
}

// Command implementations

func runInit(cmd *cobra.Command, args []string) error {
	out.Title("Initialize rift")

	// Interactive mode if no upstream provided or explicitly requested
	if upstreamURL == "" || interactive {
		out.Info("No upstream URL provided. Starting interactive setup...")
		out.Print("")

		details, err := ui.ConnectionForm(nil)
		if err != nil {
			return err
		}

		upstreamURL = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s?sslmode=%s",
			details.User,
			details.Password,
			details.Host,
			details.Port,
			details.Database,
			details.SSLMode,
		)
	}

	spinner := ui.NewSimpleSpinner("Connecting to upstream database")
	spinner.Start()

	// Connect and run migrations
	store, err := storage.New(cmd.Context(), upstreamURL)
	if err != nil {
		spinner.Stop("Connection failed")
		return fmt.Errorf("connecting to upstream: %w", err)
	}
	defer store.Close()

	if err := store.Init(cmd.Context()); err != nil {
		spinner.Stop("Migration failed")
		return fmt.Errorf("initializing storage: %w", err)
	}

	// Update main branch with the actual upstream database name
	u, _ := url.Parse(upstreamURL)
	dbName := ""
	if u != nil {
		dbName = strings.TrimPrefix(u.Path, "/")
	}
	if dbName != "" {
		mainBranch, err := store.GetBranch(cmd.Context(), "main")
		if err == nil && mainBranch.Database == "" {
			mainBranch.Database = dbName
			_ = store.UpdateBranch(cmd.Context(), mainBranch)
		}
	}

	spinner.Stop("Connected and initialized _rift schema")

	// Save config
	cfg = config.DefaultConfig()
	cfg.Upstream.URL = upstreamURL
	if dataDir != "" {
		cfg.Storage.DataDir = dataDir
	}

	configPath := cfg.Storage.DataDir + "/config.yaml"
	if err := cfg.Save(configPath); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}

	out.Success("rift initialized!")
	out.Print("")
	out.KeyValue("Config", configPath)
	out.KeyValue("Data", cfg.Storage.DataDir)
	out.Print("")
	out.Info("Next steps:")
	out.Print("  rift serve    # Start the proxy")
	out.Print("  rift create   # Create your first branch")

	return nil
}

func runServe(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	// Override config with flags
	if listenAddr != "" {
		cfg.Proxy.ListenAddr = listenAddr
	}
	if apiAddr != "" {
		cfg.API.ListenAddr = apiAddr
	}

	// Parse upstream URL to extract host:port for TCP proxy
	upstreamAddr, upstreamUser, upstreamPass := parseUpstreamURL(cfg.Upstream.URL)

	srv := server.New(&server.Config{
		UpstreamURL:    cfg.Upstream.URL,
		ListenAddr:     cfg.Proxy.ListenAddr,
		UpstreamAddr:   upstreamAddr,
		UpstreamUser:   upstreamUser,
		UpstreamPass:   upstreamPass,
		MaxConnections: cfg.Proxy.MaxConnections,
		APIAddr:        cfg.API.ListenAddr,
	})

	if err := srv.Start(cmd.Context()); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	defer func() { _ = srv.Stop() }()

	out.Title("rift")

	box := fmt.Sprintf(
		"%s Proxy:     %s\n"+
			"%s API:       %s\n"+
			"%s Upstream:  %s",
		ui.IconDatabase, cfg.Proxy.ListenAddr,
		ui.IconInfo, cfg.API.ListenAddr,
		ui.IconArrow, maskPassword(cfg.Upstream.URL),
	)
	out.Box(box)

	out.Print("")
	out.Info("Ready to accept connections")
	out.Print("")
	out.Print(ui.Muted.Render("Press Ctrl+C to stop"))

	<-cmd.Context().Done()

	out.Print("")
	out.Success("Shutdown complete")
	return nil
}

func runCreate(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	var branchName string

	if len(args) > 0 {
		branchName = args[0]
	} else if interactive || len(args) == 0 {
		// Interactive mode
		details, err := ui.BranchForm([]string{"main"})
		if err != nil {
			return err
		}
		branchName = details.Name
		parentBranch = details.Parent
		branchTTL = details.TTL
	}

	if branchName == "" {
		return fmt.Errorf("branch name is required")
	}

	spinner := ui.NewSimpleSpinner(fmt.Sprintf("Creating branch '%s'", branchName))
	spinner.Start()

	store, engine, err := connectAndInit(cmd.Context())
	if err != nil {
		spinner.Stop("Failed")
		return err
	}
	defer store.Close()

	var ttl *time.Duration
	if branchTTL != "" {
		d, err := time.ParseDuration(branchTTL)
		if err != nil {
			spinner.Stop("Failed")
			return fmt.Errorf("invalid TTL: %w", err)
		}
		ttl = &d
	}

	if err := engine.CreateBranch(cmd.Context(), branchName, parentBranch, ttl); err != nil {
		spinner.Stop("Failed")
		return fmt.Errorf("create branch: %w", err)
	}

	spinner.Stop(fmt.Sprintf("Branch '%s' created", branchName))

	out.Print("")
	out.KeyValue("Parent", parentBranch)
	if branchTTL != "" {
		out.KeyValue("TTL", branchTTL)
	}
	out.Print("")
	out.Info("Connect with:")
	out.Print(fmt.Sprintf("  psql postgres://localhost:6432/%s", branchName))

	return nil
}

func runDelete(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	branchName := args[0]

	if !forceDelete {
		confirmed, err := ui.Confirm(
			fmt.Sprintf("Delete branch '%s'? This cannot be undone.", branchName),
			false,
		)
		if err != nil {
			return err
		}
		if !confirmed {
			out.Info("Cancelled")
			return nil
		}
	}

	spinner := ui.NewSimpleSpinner(fmt.Sprintf("Deleting branch '%s'", branchName))
	spinner.Start()

	store, engine, err := connectAndInit(cmd.Context())
	if err != nil {
		spinner.Stop("Failed")
		return err
	}
	defer store.Close()

	if err := engine.DeleteBranch(cmd.Context(), branchName); err != nil {
		spinner.Stop("Failed")
		return fmt.Errorf("delete branch: %w", err)
	}

	spinner.Stop(fmt.Sprintf("Branch '%s' deleted", branchName))
	return nil
}

func runList(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	store, err := storage.New(cmd.Context(), cfg.Upstream.URL)
	if err != nil {
		return fmt.Errorf("connect to upstream: %w", err)
	}
	defer store.Close()

	branches, err := store.ListBranches(cmd.Context())
	if err != nil {
		return fmt.Errorf("list branches: %w", err)
	}

	if output == "json" || output == "yaml" {
		return out.Data(branches)
	}

	table := ui.NewTable(out, "NAME", "PARENT", "CREATED", "ROWS CHANGED", "STATUS")
	for _, b := range branches {
		parent := b.Parent
		if parent == "" {
			parent = "-"
		}
		created := b.CreatedAt.Format("2006-01-02 15:04")
		status := ui.Success.Render("● " + b.Status)
		table.AddRow(b.Name, parent, created, fmt.Sprintf("%d", b.RowsChanged), status)
	}
	table.Render()

	return nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	store, err := storage.New(cmd.Context(), cfg.Upstream.URL)
	if err != nil {
		return fmt.Errorf("connect to upstream: %w", err)
	}
	defer store.Close()

	if len(args) > 0 {
		branchName := args[0]
		b, err := store.GetBranch(cmd.Context(), branchName)
		if err != nil {
			return fmt.Errorf("branch %q not found", branchName)
		}

		out.Title(fmt.Sprintf("Branch: %s", branchName))

		parent := b.Parent
		if parent == "" {
			parent = "-"
		}
		out.KeyValue("Parent", parent)
		out.KeyValue("Created", b.CreatedAt.Format("2006-01-02 15:04:05"))
		out.KeyValue("Updated", b.UpdatedAt.Format("2006-01-02 15:04:05"))
		out.KeyValue("Rows changed", fmt.Sprintf("%d", b.RowsChanged))
		out.KeyValue("Delta size", fmt.Sprintf("%d bytes", b.DeltaSize))
		out.KeyValue("Pinned", fmt.Sprintf("%v", b.Pinned))
		out.KeyValue("Status", ui.Success.Render(b.Status))

		// Show tracked tables
		tables, err := store.ListTrackedTables(cmd.Context(), branchName)
		if err == nil && len(tables) > 0 {
			out.Print("")
			out.Info("Tracked tables:")
			for _, t := range tables {
				out.Print(fmt.Sprintf("  %s.%s (rows: %d)", t.SourceSchema, t.TableName, t.RowCount))
			}
		}
	} else {
		out.Title("rift Status")

		branches, err := store.ListBranches(cmd.Context())
		if err != nil {
			return err
		}

		out.KeyValue("Upstream", ui.Success.Render("● connected"))
		out.Print("")
		out.KeyValue("Branches", fmt.Sprintf("%d", len(branches)))
	}

	return nil
}

func runDiff(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	branchName := args[0]

	store, engine, err := connectAndInit(cmd.Context())
	if err != nil {
		return err
	}
	defer store.Close()

	diff, err := engine.Diff(cmd.Context(), branchName)
	if err != nil {
		return fmt.Errorf("compute diff: %w", err)
	}

	out.Title(fmt.Sprintf("Diff: %s → %s", branchName, diff.Parent))

	if len(diff.Tables) == 0 {
		out.Info("No changes")
		return nil
	}

	out.Info("Data changes:")
	for _, t := range diff.Tables {
		out.Print(fmt.Sprintf("  %s: %d inserts, %d updates, %d deletes",
			t.TableName, t.Inserts, t.Updates, t.Deletes))
	}

	out.Print("")
	out.KeyValue("Total changes", fmt.Sprintf("%d", diff.TotalChanges()))

	return nil
}

func runMerge(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("rift not initialized. Run 'rift init' first")
	}

	branchName := args[0]

	store, engine, err := connectAndInit(cmd.Context())
	if err != nil {
		return err
	}
	defer store.Close()

	merges, err := engine.GenerateMerge(cmd.Context(), branchName)
	if err != nil {
		return fmt.Errorf("generate merge: %w", err)
	}

	if len(merges) == 0 {
		out.Info("No changes to merge")
		return nil
	}

	out.Title(fmt.Sprintf("Merge: %s → parent", branchName))

	if dryRun {
		out.Warning("Dry run - displaying SQL only")
		out.Print("")
	}

	out.Print("-- Generated merge SQL")
	for _, m := range merges {
		out.Print(fmt.Sprintf("-- Table: %s", m.TableName))
		out.Print(cow.FormatMergeSQL(&m))
		out.Print("")
	}

	return nil
}

func runConnect(cmd *cobra.Command, args []string) error {
	branchName := args[0]

	addr := ":6432"
	if cfg != nil && cfg.Proxy.ListenAddr != "" {
		addr = cfg.Proxy.ListenAddr
	}

	connURL := fmt.Sprintf("postgres://localhost%s/%s", addr, branchName)
	out.Info(fmt.Sprintf("Connecting to branch '%s'...", branchName))
	out.Print(fmt.Sprintf("  psql %s", connURL))

	// Find psql binary
	psqlPath, err := findExecutable("psql")
	if err != nil {
		return fmt.Errorf("psql not found in PATH. Connect manually:\n  psql %s", connURL)
	}

	// Replace process with psql
	return syscall.Exec(psqlPath, []string{"psql", connURL}, os.Environ()) // #nosec G204 -- user-controlled branch name is validated
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("no configuration loaded")
	}
	return out.YAML(cfg)
}

func runConfigSet(cmd *cobra.Command, args []string) error {
	if cfg == nil {
		return fmt.Errorf("no configuration loaded")
	}

	key := args[0]
	value := args[1]

	viper.Set(key, value)

	configPath := viper.ConfigFileUsed()
	if configPath == "" {
		configPath = cfg.Storage.DataDir + "/config.yaml"
	}

	if err := cfg.Save(configPath); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}

	out.Success(fmt.Sprintf("Set %s = %s", key, value))
	return nil
}

// connectAndInit creates a storage connection and CoW engine for CLI commands.
func connectAndInit(ctx context.Context) (storage.Store, *cow.Engine, error) {
	store, err := storage.New(ctx, cfg.Upstream.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to upstream: %w", err)
	}
	engine := cow.NewEngine(store)
	return store, engine, nil
}

// parseUpstreamURL extracts host:port, user, and password from a Postgres URL.
func parseUpstreamURL(rawURL string) (addr, user, pass string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "localhost:5432", "", ""
	}

	host := u.Hostname()
	port := u.Port()
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "5432"
	}
	addr = host + ":" + port

	user = u.User.Username()
	pass, _ = u.User.Password()

	return addr, user, pass
}

// maskPassword masks the password in a URL for display.
func maskPassword(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	if u.User == nil {
		return rawURL
	}
	if _, hasPass := u.User.Password(); hasPass {
		u.User = url.UserPassword(u.User.Username(), "****")
	}
	return u.String()
}

// findExecutable locates a binary in PATH.
func findExecutable(name string) (string, error) {
	return exec.LookPath(name)
}
