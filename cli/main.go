package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type config struct {
	UseExistingRedis bool   `json:"useExistingRedis"`
	RedisAddr        string `json:"redisAddr"`
	RedisPassword    string `json:"redisPassword"`
	RedisDB          int    `json:"redisDB"`
	RedisKey         string `json:"redisKey"`
	Mountpoint       string `json:"mountpoint"`
	ReadOnly         bool   `json:"readOnly"`
	AllowOther       bool   `json:"allowOther"`
	RedisServerBin   string `json:"redisServerBin"`
	ModulePath       string `json:"modulePath"`
	MountBin         string `json:"mountBin"`
	RedisLog         string `json:"redisLog"`
	MountLog         string `json:"mountLog"`

	// Derived at runtime, not persisted.
	redisHost string
	redisPort int
}

type state struct {
	StartedAt      time.Time `json:"started_at"`
	ManageRedis    bool      `json:"manage_redis"`
	RedisPID       int       `json:"redis_pid"`
	RedisAddr      string    `json:"redis_addr"`
	RedisDB        int       `json:"redis_db"`
	MountPID       int       `json:"mount_pid"`
	Mountpoint     string    `json:"mountpoint"`
	RedisKey       string    `json:"redis_key"`
	RedisLog       string    `json:"redis_log"`
	MountLog       string    `json:"mount_log"`
	RedisServerBin string    `json:"redis_server_bin"`
	MountBin       string    `json:"mount_bin"`
	ArchivePath    string    `json:"archive_path,omitempty"`
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

var cfgPathOverride string

func main() {
	defer showCursor()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		showCursor()
		fmt.Println()
		os.Exit(130)
	}()

	args := os.Args[1:]
	if len(args) >= 2 && args[0] == "--config" {
		cfgPathOverride = args[1]
		args = args[2:]
	}

	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "setup":
		if err := cmdSetup(); err != nil {
			fatal(err)
		}
	case "up":
		if err := cmdUp(); err != nil {
			fatal(err)
		}
	case "down":
		if err := cmdDown(); err != nil {
			fatal(err)
		}
	case "status":
		if err := cmdStatus(); err != nil {
			fatal(err)
		}
	case "migrate":
		if err := cmdMigrate(args); err != nil {
			fatal(err)
		}
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	printBannerCompact()
	bin := filepath.Base(os.Args[0])
	fmt.Fprintf(os.Stderr, `Usage:
  %s [--config <path>] <command>

Commands:
  setup                First-time interactive setup
  up                   Start the filesystem
  down                 Stop and unmount
  status               Show current status
  migrate <directory>  Migrate a directory into Redis

Config: %s
`, bin, configPath())
}

// ---------------------------------------------------------------------------
// setup — interactive wizard → save config → start
// ---------------------------------------------------------------------------

func cmdSetup() error {
	if st, err := loadState(); err == nil {
		if st.MountPID > 0 && processAlive(st.MountPID) {
			return fmt.Errorf("redis-fs is currently running\nRun '%s down' first", filepath.Base(os.Args[0]))
		}
	}

	printBanner()

	fmt.Println("  " + clr(ansiDim, "Redis-FS stores an entire filesystem inside a single Redis"))
	fmt.Println("  " + clr(ansiDim, "key. Files, directories, and metadata are kept in memory and"))
	fmt.Println("  " + clr(ansiDim, "accessible via a FUSE mount on your local machine."))
	fmt.Println()
	fmt.Println("  " + clr(ansiBold, "Let's get you set up."))
	fmt.Println()

	r := bufio.NewReader(os.Stdin)
	cfg, migrateDir, err := runSetupWizard(r, os.Stdout)
	if err != nil {
		return err
	}

	if err := resolveConfigPaths(&cfg); err != nil {
		return err
	}

	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("  %s Saved to %s\n\n", clr(ansiDim, "▸"), clr(ansiCyan, configPath()))

	if migrateDir != "" {
		return performMigration(cfg, migrateDir, r)
	}
	return startServices(cfg)
}

func runSetupWizard(r *bufio.Reader, out io.Writer) (config, string, error) {
	cfg := config{
		RedisAddr: "localhost:6379",
		RedisDB:   0,
		RedisKey:  "myfs",
		RedisLog:  "/tmp/rfs-redis.log",
		MountLog:  "/tmp/rfs-mount.log",
	}

	// ── Redis connection ────────────────────────────────
	fmt.Fprintln(out, "  "+clr(ansiBold+ansiCyan, "▸")+" "+clr(ansiBold, "Redis Connection"))
	fmt.Fprintln(out)

	useExisting, err := promptYesNo(r, out,
		"  Do you have a Redis server you'd like to connect to?\n"+
			"  "+clr(ansiDim, "If not, we'll start and manage one for you"), false)
	if err != nil {
		return cfg, "", err
	}
	cfg.UseExistingRedis = useExisting

	if cfg.UseExistingRedis {
		addr, err := promptString(r, out,
			"\n  Redis server address\n"+
				"  "+clr(ansiDim, "Format: host:port"), cfg.RedisAddr)
		if err != nil {
			return cfg, "", err
		}
		cfg.RedisAddr = addr

		pwd, err := promptString(r, out,
			"\n  Redis password\n"+
				"  "+clr(ansiDim, "Leave empty if none"), "")
		if err != nil {
			return cfg, "", err
		}
		cfg.RedisPassword = pwd
	}

	// ── Filesystem ──────────────────────────────────────
	fmt.Fprintln(out)
	fmt.Fprintln(out, "  "+clr(ansiBold+ansiCyan, "▸")+" "+clr(ansiBold, "Filesystem"))
	fmt.Fprintln(out)

	key, err := promptString(r, out,
		"  What do you want to call this filesystem?\n"+
			"  "+clr(ansiDim, "Each filesystem is stored as a single key; you can have many"), cfg.RedisKey)
	if err != nil {
		return cfg, "", err
	}
	cfg.RedisKey = key

	// ── New mount vs. migrate ───────────────────────────
	fmt.Fprintln(out)
	fmt.Fprintln(out, "  How would you like to start?")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "    "+clr(ansiCyan, "1")+"  Create a new empty mount point")
	fmt.Fprintln(out, "    "+clr(ansiCyan, "2")+"  Migrate an existing directory into Redis")
	fmt.Fprintln(out)

	choice, err := promptString(r, out, "  Choose", "1")
	if err != nil {
		return cfg, "", err
	}

	var migrateDir string

	if choice == "2" {
		dir, err := promptString(r, out,
			"\n  Which directory would you like to migrate?\n"+
				"  "+clr(ansiDim, "The original will be archived and replaced with the Redis mount"), "")
		if err != nil {
			return cfg, "", err
		}
		if dir == "" {
			return cfg, "", errors.New("directory path is required")
		}
		dir, err = expandPath(dir)
		if err != nil {
			return cfg, "", err
		}
		fi, err := os.Stat(dir)
		if err != nil {
			return cfg, "", fmt.Errorf("cannot access %s: %w", dir, err)
		}
		if !fi.IsDir() {
			return cfg, "", fmt.Errorf("%s is not a directory", dir)
		}
		if isMounted(dir) {
			return cfg, "", fmt.Errorf("%s is already a FUSE mountpoint", dir)
		}
		cfg.Mountpoint = dir
		cfg.RedisKey = filepath.Base(dir)
		migrateDir = dir
	} else {
		mp, err := promptString(r, out,
			"\n  Where should the filesystem be mounted?", "~/redis-fs")
		if err != nil {
			return cfg, "", err
		}
		cfg.Mountpoint, err = expandPath(mp)
		if err != nil {
			return cfg, "", err
		}
	}

	fmt.Fprintln(out)
	return cfg, migrateDir, nil
}

// ---------------------------------------------------------------------------
// up — load config and start services
// ---------------------------------------------------------------------------

func cmdUp() error {
	if st, err := loadState(); err == nil {
		if st.MountPID > 0 && processAlive(st.MountPID) {
			return fmt.Errorf("redis-fs is already running (pid %d, mounted at %s)\nRun '%s down' first",
				st.MountPID, st.Mountpoint, filepath.Base(os.Args[0]))
		}
	}

	cfg, err := loadConfig()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no configuration found\nRun '%s setup' first, or create %s manually",
				filepath.Base(os.Args[0]), configPath())
		}
		return err
	}

	if err := resolveConfigPaths(&cfg); err != nil {
		return err
	}

	printBanner()
	return startServices(cfg)
}

// ---------------------------------------------------------------------------
// down — stop services
// ---------------------------------------------------------------------------

func cmdDown() error {
	st, err := loadState()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fmt.Println("\n  Redis-FS is not running. Nothing to stop.\n")
			return nil
		}
		return err
	}

	fmt.Println()

	if isMounted(st.Mountpoint) {
		s := startStep("Unmounting filesystem")
		if err := unmount(st.Mountpoint); err != nil {
			s.fail(err.Error())
			return fmt.Errorf("unmount %s: %w", st.Mountpoint, err)
		}
		s.succeed(st.Mountpoint)
	}

	if st.MountPID > 0 && processAlive(st.MountPID) {
		s := startStep("Stopping mount daemon")
		_ = terminatePID(st.MountPID, 2*time.Second)
		s.succeed(fmt.Sprintf("pid %d", st.MountPID))
	}

	if st.ManageRedis && st.RedisPID > 0 && processAlive(st.RedisPID) {
		s := startStep("Stopping Redis server")
		_ = terminatePID(st.RedisPID, 2*time.Second)
		s.succeed(fmt.Sprintf("pid %d", st.RedisPID))
	}

	if err := os.Remove(statePath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	fmt.Printf("\n  %s redis-fs stopped\n\n", clr(ansiDim, "■"))
	return nil
}

// ---------------------------------------------------------------------------
// status — show current state
// ---------------------------------------------------------------------------

func cmdStatus() error {
	st, err := loadState()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			title := clr(ansiDim, "○") + " redis-fs is not running"
			printBox(title, []boxRow{
				{Label: "start", Value: clr(ansiCyan, "rfs up")},
			})
			return nil
		}
		return err
	}

	mounted := isMounted(st.Mountpoint)
	mountAlive := st.MountPID > 0 && processAlive(st.MountPID)

	var title string
	if mounted && mountAlive {
		title = clr(ansiBGreen, "●") + " " + clr(ansiBold, "redis-fs is running")
	} else {
		title = clr(ansiYellow, "○") + " redis-fs is stopped"
	}

	rows := []boxRow{
		{Label: "uptime", Value: formatDuration(time.Since(st.StartedAt))},
		{Label: "mount", Value: st.Mountpoint},
		{Label: "key", Value: st.RedisKey},
		{Label: "redis", Value: fmt.Sprintf("%s (db %d)", st.RedisAddr, st.RedisDB)},
	}

	if st.ManageRedis {
		rows = append(rows, boxRow{Label: "redis pid", Value: pidStatusColored(st.RedisPID)})
	}
	rows = append(rows, boxRow{Label: "mount pid", Value: pidStatusColored(st.MountPID)})

	mountState := clr(ansiRed, "not mounted")
	if mounted {
		mountState = clr(ansiGreen, "mounted")
	}
	rows = append(rows, boxRow{Label: "state", Value: mountState})

	if st.ArchivePath != "" {
		rows = append(rows, boxRow{Label: "archive", Value: st.ArchivePath})
	}

	printBox(title, rows)
	return nil
}

// ---------------------------------------------------------------------------
// migrate — import a directory (reads saved config for Redis settings)
// ---------------------------------------------------------------------------

func cmdMigrate(args []string) error {
	if st, err := loadState(); err == nil {
		if st.MountPID > 0 && processAlive(st.MountPID) {
			return fmt.Errorf("redis-fs is currently running\nRun '%s down' first", filepath.Base(os.Args[0]))
		}
	}

	if len(args) < 2 {
		return fmt.Errorf("missing directory\n\nUsage: %s migrate <directory>", filepath.Base(os.Args[0]))
	}

	sourceDir, err := expandPath(args[1])
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	fi, err := os.Stat(sourceDir)
	if err != nil {
		return fmt.Errorf("cannot access %s: %w", sourceDir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", sourceDir)
	}
	if isMounted(sourceDir) {
		return fmt.Errorf("%s is already a FUSE mountpoint", sourceDir)
	}

	cfg, err := loadConfig()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("no configuration found\nRun '%s setup' first", filepath.Base(os.Args[0]))
		}
		return err
	}

	cfg.Mountpoint = sourceDir
	cfg.RedisKey = filepath.Base(sourceDir)

	if err := resolveConfigPaths(&cfg); err != nil {
		return err
	}
	if err := saveConfig(cfg); err != nil {
		return err
	}

	printBanner()
	return performMigration(cfg, sourceDir, bufio.NewReader(os.Stdin))
}

// ---------------------------------------------------------------------------
// Service lifecycle
// ---------------------------------------------------------------------------

func startServices(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	redisPID := 0
	if !cfg.UseExistingRedis {
		s := startStep("Starting Redis server")
		pid, err := startRedisDaemon(cfg)
		if err != nil {
			s.fail(err.Error())
			return err
		}
		redisPID = pid
		s.succeed(fmt.Sprintf("pid %d", pid))
	}

	s := startStep("Connecting to Redis")
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: 4,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		s.fail(fmt.Sprintf("cannot reach %s", cfg.RedisAddr))
		return fmt.Errorf("cannot connect to Redis at %s: %w", cfg.RedisAddr, err)
	}
	s.succeed(cfg.RedisAddr)

	s = startStep("Checking FS module")
	if err := ensureFSModuleLoaded(ctx, rdb); err != nil {
		s.fail("module not loaded")
		return err
	}
	s.succeed("ready")

	s = startStep("Mounting filesystem")
	if err := os.MkdirAll(cfg.Mountpoint, 0o755); err != nil {
		s.fail(err.Error())
		return fmt.Errorf("create mountpoint: %w", err)
	}
	if err := rdb.Do(ctx, "FS.TOUCH", cfg.RedisKey, "/.mount-check").Err(); err != nil {
		s.fail(err.Error())
		return fmt.Errorf("failed to initialize key %q: %w", cfg.RedisKey, err)
	}

	mpid, err := startMountDaemon(cfg)
	if err != nil {
		s.fail(err.Error())
		return err
	}
	if err := waitForMount(cfg.Mountpoint, 6*time.Second); err != nil {
		s.fail("timeout")
		return fmt.Errorf("mount did not become ready: %w", err)
	}
	s.succeed(cfg.Mountpoint)

	st := state{
		StartedAt:      time.Now().UTC(),
		ManageRedis:    !cfg.UseExistingRedis,
		RedisAddr:      cfg.RedisAddr,
		RedisDB:        cfg.RedisDB,
		MountPID:       mpid,
		Mountpoint:     cfg.Mountpoint,
		RedisKey:       cfg.RedisKey,
		RedisLog:       cfg.RedisLog,
		MountLog:       cfg.MountLog,
		RedisServerBin: cfg.RedisServerBin,
		MountBin:       cfg.MountBin,
	}
	if !cfg.UseExistingRedis {
		st.RedisPID = redisPID
	}
	if err := saveState(st); err != nil {
		return err
	}

	printReadyBox(cfg)
	return nil
}

func printReadyBox(cfg config) {
	title := clr(ansiBGreen, "●") + " " + clr(ansiBold, "redis-fs is ready")
	rows := []boxRow{
		{Label: "mount", Value: cfg.Mountpoint},
		{Label: "key", Value: cfg.RedisKey},
		{Label: "redis", Value: fmt.Sprintf("%s (db %d)", cfg.RedisAddr, cfg.RedisDB)},
	}
	if cfg.ReadOnly {
		rows = append(rows, boxRow{Label: "mode", Value: "read-only"})
	}
	rows = append(rows, boxRow{})
	rows = append(rows, boxRow{Label: "try", Value: clr(ansiCyan, "ls "+cfg.Mountpoint)})
	rows = append(rows, boxRow{Label: "stop", Value: clr(ansiCyan, filepath.Base(os.Args[0])+" down")})
	rows = append(rows, boxRow{Label: "config", Value: clr(ansiDim, configPath())})
	printBox(title, rows)
}

func performMigration(cfg config, sourceDir string, r *bufio.Reader) error {
	archiveDir := sourceDir + ".archive"

	planTitle := clr(ansiBold, "Migration plan")
	printBox(planTitle, []boxRow{
		{Label: "source", Value: sourceDir},
		{Label: "archive", Value: archiveDir},
		{Label: "key", Value: cfg.RedisKey},
		{Label: "redis", Value: fmt.Sprintf("%s (db %d)", cfg.RedisAddr, cfg.RedisDB)},
		{},
		{Value: clr(ansiDim, "1.") + " Import all files into Redis"},
		{Value: clr(ansiDim, "2.") + " Move original to archive"},
		{Value: clr(ansiDim, "3.") + " Mount Redis FS in place"},
	})

	ok, err := promptYesNo(r, os.Stdout, "  Proceed?", false)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("migration cancelled")
	}
	fmt.Println()

	redisPID := 0
	if !cfg.UseExistingRedis {
		s := startStep("Starting Redis server")
		pid, err := startRedisDaemon(cfg)
		if err != nil {
			s.fail(err.Error())
			return err
		}
		redisPID = pid
		s.succeed(fmt.Sprintf("pid %d", pid))
	}

	step := startStep("Connecting to Redis")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
		PoolSize: 8,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		step.fail(fmt.Sprintf("cannot reach %s", cfg.RedisAddr))
		return fmt.Errorf("cannot connect to Redis at %s: %w", cfg.RedisAddr, err)
	}
	step.succeed(cfg.RedisAddr)

	step = startStep("Checking FS module")
	if err := ensureFSModuleLoaded(ctx, rdb); err != nil {
		step.fail("not loaded")
		return err
	}
	step.succeed("ready")

	exists, err := rdb.Exists(ctx, cfg.RedisKey).Result()
	if err != nil {
		return err
	}
	if exists > 0 {
		ok, err := promptYesNo(r, os.Stdout,
			fmt.Sprintf("  Redis key %q already exists. Overwrite?", cfg.RedisKey), false)
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("migration cancelled")
		}
		if err := rdb.Del(ctx, cfg.RedisKey).Err(); err != nil {
			return fmt.Errorf("delete existing redis key: %w", err)
		}
	}

	step = startStep("Importing files")
	files, dirs, links, err := importDirectory(ctx, rdb, cfg.RedisKey, sourceDir, func(f, d, l int) {
		label := fmt.Sprintf("Importing · %d files, %d dirs", f, d)
		if l > 0 {
			label += fmt.Sprintf(", %d symlinks", l)
		}
		step.update(label)
	})
	if err != nil {
		step.fail(err.Error())
		return err
	}
	detail := fmt.Sprintf("%d files, %d dirs", files, dirs)
	if links > 0 {
		detail += fmt.Sprintf(", %d symlinks", links)
	}
	step.succeed(detail)

	if _, err := os.Stat(archiveDir); err == nil {
		return fmt.Errorf("archive path already exists: %s", archiveDir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	step = startStep("Archiving original directory")
	if err := os.Rename(sourceDir, archiveDir); err != nil {
		step.fail(err.Error())
		return fmt.Errorf("archive failed: %w", err)
	}
	step.succeed(archiveDir)

	rollback := true
	defer func() {
		if rollback {
			_ = os.RemoveAll(sourceDir)
			_ = os.Rename(archiveDir, sourceDir)
		}
	}()

	step = startStep("Mounting filesystem")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		step.fail(err.Error())
		return err
	}

	mpid, err := startMountDaemon(cfg)
	if err != nil {
		step.fail(err.Error())
		return err
	}
	if err := waitForMount(cfg.Mountpoint, 8*time.Second); err != nil {
		step.fail("timeout")
		return err
	}
	step.succeed(cfg.Mountpoint)

	st := state{
		StartedAt:      time.Now().UTC(),
		ManageRedis:    !cfg.UseExistingRedis,
		RedisPID:       redisPID,
		RedisAddr:      cfg.RedisAddr,
		RedisDB:        cfg.RedisDB,
		MountPID:       mpid,
		Mountpoint:     cfg.Mountpoint,
		RedisKey:       cfg.RedisKey,
		RedisLog:       cfg.RedisLog,
		MountLog:       cfg.MountLog,
		RedisServerBin: cfg.RedisServerBin,
		MountBin:       cfg.MountBin,
		ArchivePath:    archiveDir,
	}
	if err := saveState(st); err != nil {
		return err
	}
	rollback = false

	title := clr(ansiBGreen, "●") + " " + clr(ansiBold, "migration complete")
	printBox(title, []boxRow{
		{Label: "archive", Value: archiveDir},
		{Label: "mount", Value: cfg.Mountpoint},
		{Label: "key", Value: cfg.RedisKey},
		{},
		{Label: "try", Value: clr(ansiCyan, "ls "+cfg.Mountpoint)},
		{Label: "stop", Value: clr(ansiCyan, filepath.Base(os.Args[0])+" down")},
		{Label: "config", Value: clr(ansiDim, configPath())},
	})
	return nil
}

// ---------------------------------------------------------------------------
// Directory import
// ---------------------------------------------------------------------------

func importDirectory(ctx context.Context, rdb *redis.Client, key, source string, onProgress func(files, dirs, symlinks int)) (int, int, int, error) {
	var files, dirs, symlinks int
	err := filepath.WalkDir(source, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == source {
			return nil
		}

		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		redisPath := "/" + filepath.ToSlash(rel)

		info, err := os.Lstat(path)
		if err != nil {
			return err
		}

		switch {
		case d.Type()&os.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if err := rdb.Do(ctx, "FS.LN", key, target, redisPath).Err(); err != nil {
				return fmt.Errorf("FS.LN %s: %w", redisPath, err)
			}
			symlinks++
		case d.IsDir():
			if err := rdb.Do(ctx, "FS.MKDIR", key, redisPath, "PARENTS").Err(); err != nil {
				return fmt.Errorf("FS.MKDIR %s: %w", redisPath, err)
			}
			dirs++
		default:
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if err := rdb.Do(ctx, "FS.ECHO", key, redisPath, data).Err(); err != nil {
				return fmt.Errorf("FS.ECHO %s: %w", redisPath, err)
			}
			files++
		}

		if err := applyMetadata(ctx, rdb, key, redisPath, info); err != nil {
			return err
		}
		if onProgress != nil {
			onProgress(files, dirs, symlinks)
		}
		return nil
	})
	return files, dirs, symlinks, err
}

func applyMetadata(ctx context.Context, rdb *redis.Client, key, path string, info os.FileInfo) error {
	modeStr := fmt.Sprintf("%04o", info.Mode().Perm())
	if err := rdb.Do(ctx, "FS.CHMOD", key, path, modeStr).Err(); err != nil {
		return fmt.Errorf("FS.CHMOD %s: %w", path, err)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		if err := rdb.Do(ctx, "FS.CHOWN", key, path, st.Uid, st.Gid).Err(); err != nil {
			return fmt.Errorf("FS.CHOWN %s: %w", path, err)
		}
		aSec, aNsec := statAtime(st)
		mSec, mNsec := statMtime(st)
		atimeMs := aSec*1000 + aNsec/1_000_000
		mtimeMs := mSec*1000 + mNsec/1_000_000
		if err := rdb.Do(ctx, "FS.UTIMENS", key, path, atimeMs, mtimeMs).Err(); err != nil {
			return fmt.Errorf("FS.UTIMENS %s: %w", path, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Daemon management
// ---------------------------------------------------------------------------

func startRedisDaemon(cfg config) (int, error) {
	pidfile := fmt.Sprintf("/tmp/rfs-%d.pid", cfg.redisPort)
	args := []string{
		"--port", strconv.Itoa(cfg.redisPort),
		"--loadmodule", cfg.ModulePath,
		"--save", "",
		"--appendonly", "no",
		"--daemonize", "yes",
		"--pidfile", pidfile,
		"--logfile", cfg.RedisLog,
		"--dir", "/tmp",
		"--dbfilename", fmt.Sprintf("rfs-%d.rdb", cfg.redisPort),
	}
	cmd := exec.Command(cfg.RedisServerBin, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return 0, fmt.Errorf("start redis failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		pidBytes, err := os.ReadFile(pidfile)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
			if err == nil && pid > 0 {
				return pid, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, errors.New("redis started but pidfile was not found")
}

func startMountDaemon(cfg config) (int, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.MountLog), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(cfg.MountLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, err
	}

	args := []string{
		"--redis", cfg.RedisAddr,
		"--db", strconv.Itoa(cfg.RedisDB),
		"--foreground",
		cfg.RedisKey,
		cfg.Mountpoint,
	}
	if cfg.RedisPassword != "" {
		args = append([]string{"--password", cfg.RedisPassword}, args...)
	}
	if cfg.ReadOnly {
		args = append([]string{"--readonly"}, args...)
	}
	if cfg.AllowOther {
		args = append([]string{"--allow-other"}, args...)
	}

	cmd := exec.Command(cfg.MountBin, args...)
	cmd.Stdout = f
	cmd.Stderr = f
	devNull, err := os.Open(os.DevNull)
	if err == nil {
		defer devNull.Close()
		cmd.Stdin = devNull
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("start mount failed: %w", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	_ = f.Close()
	return pid, nil
}

func ensureFSModuleLoaded(ctx context.Context, rdb *redis.Client) error {
	res, err := rdb.Do(ctx, "COMMAND", "LIST", "FILTERBY", "MODULE", "fs").Slice()
	if err != nil {
		return fmt.Errorf("module capability check failed: %w", err)
	}
	if len(res) > 0 {
		return nil
	}
	return errors.New("Redis FS module is not loaded.\nLoad it with: redis-cli MODULE LOAD /path/to/module/fs.so")
}

func waitForMount(mountpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isMounted(mountpoint) {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return errors.New("timeout waiting for mount")
}

func isMounted(mountpoint string) bool {
	b, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	return strings.Contains(string(b), " "+mountpoint+" ")
}

func unmount(mountpoint string) error {
	for _, c := range [][]string{
		{"fusermount", "-u", mountpoint},
		{"fusermount", "-uz", mountpoint},
		{"umount", "-l", mountpoint},
	} {
		if exec.Command(c[0], c[1:]...).Run() == nil {
			return nil
		}
	}
	return errors.New("all unmount commands failed")
}

func terminatePID(pid int, timeout time.Duration) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	_ = p.Signal(syscall.SIGTERM)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = p.Signal(syscall.SIGKILL)
	return nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

// ---------------------------------------------------------------------------
// Config persistence (~/.rfs/config.json)
// ---------------------------------------------------------------------------

func configPath() string {
	if cfgPathOverride != "" {
		return cfgPathOverride
	}
	exe, err := os.Executable()
	if err != nil {
		return "rfs.config.json"
	}
	return filepath.Join(filepath.Dir(exe), "rfs.config.json")
}

func saveConfig(cfg config) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), b, 0o644)
}

func loadConfig() (config, error) {
	cfg := config{
		RedisAddr: "localhost:6379",
		RedisDB:   0,
		RedisKey:  "myfs",
		RedisLog:  "/tmp/rfs-redis.log",
		MountLog:  "/tmp/rfs-mount.log",
	}
	b, err := os.ReadFile(configPath())
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func resolveConfigPaths(cfg *config) error {
	dir := exeDir()

	if cfg.Mountpoint != "" {
		mp, err := expandPath(cfg.Mountpoint)
		if err != nil {
			return err
		}
		cfg.Mountpoint = mp
	}

	if cfg.MountBin == "" {
		defMountBin := filepath.Join(dir, "mount", "redis-fs-mount")
		if _, err := os.Stat(defMountBin); err != nil {
			defMountBin = "redis-fs-mount"
		}
		resolved, err := resolveBinary(defMountBin)
		if err != nil {
			return fmt.Errorf("cannot find redis-fs-mount binary\n  Build it with: make mount")
		}
		cfg.MountBin = resolved
	}

	if !cfg.UseExistingRedis {
		if cfg.RedisServerBin == "" {
			resolved, err := resolveBinary(defaultRedisBin())
			if err != nil {
				return fmt.Errorf("cannot find redis-server binary\n  Install Redis or set useExistingRedis to true in config")
			}
			cfg.RedisServerBin = resolved
		}
		if cfg.ModulePath == "" {
			cfg.ModulePath = filepath.Join(dir, "module", "fs.so")
		}
		if _, err := os.Stat(cfg.ModulePath); err != nil {
			return fmt.Errorf("cannot find fs.so module at %s\n  Build it with: make module", cfg.ModulePath)
		}
	}

	host, port, err := splitAddr(cfg.RedisAddr)
	if err != nil {
		return err
	}
	cfg.redisHost = host
	cfg.redisPort = port

	return nil
}

// ---------------------------------------------------------------------------
// State persistence (~/.rfs/state.json)
// ---------------------------------------------------------------------------

func stateDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".rfs")
}

func statePath() string {
	return filepath.Join(stateDir(), "state.json")
}

func saveState(st state) error {
	if err := os.MkdirAll(stateDir(), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(statePath(), b, 0o600)
}

func loadState() (state, error) {
	var st state
	b, err := os.ReadFile(statePath())
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, err
	}
	return st, nil
}

// ---------------------------------------------------------------------------
// Prompt helpers
// ---------------------------------------------------------------------------

func promptString(r *bufio.Reader, out io.Writer, label, def string) (string, error) {
	if def != "" {
		fmt.Fprintf(out, "%s [%s]: ", label, clr(ansiCyan, def))
	} else {
		fmt.Fprintf(out, "%s: ", label)
	}
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	v := strings.TrimSpace(line)
	if v == "" {
		return def, nil
	}
	return v, nil
}

func promptYesNo(r *bufio.Reader, out io.Writer, label string, def bool) (bool, error) {
	defMark := "y/N"
	if def {
		defMark = "Y/n"
	}
	fmt.Fprintf(out, "%s [%s]: ", label, clr(ansiCyan, defMark))
	line, err := r.ReadString('\n')
	if err != nil {
		return false, err
	}
	v := strings.ToLower(strings.TrimSpace(line))
	if v == "" {
		return def, nil
	}
	if v == "y" || v == "yes" {
		return true, nil
	}
	if v == "n" || v == "no" {
		return false, nil
	}
	return def, nil
}

// ---------------------------------------------------------------------------
// Path / binary helpers
// ---------------------------------------------------------------------------

func splitAddr(addr string) (string, int, error) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid address %q (expected host:port)", addr)
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, err
	}
	return parts[0], p, nil
}

func expandPath(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		p = filepath.Join(home, p[2:])
	}
	return filepath.Abs(p)
}

func resolveBinary(p string) (string, error) {
	if strings.Contains(p, "/") {
		return expandPath(p)
	}
	lp, err := exec.LookPath(p)
	if err != nil {
		return "", fmt.Errorf("binary %q not found in PATH", p)
	}
	return lp, nil
}

func exeDir() string {
	exe, err := os.Executable()
	if err != nil {
		cwd, _ := os.Getwd()
		return cwd
	}
	return filepath.Dir(exe)
}

func defaultRedisBin() string {
	candidate := filepath.Join(os.Getenv("HOME"), "git", "redis", "src", "redis-server")
	if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
		return candidate
	}
	if lp, err := exec.LookPath("redis-server"); err == nil {
		return lp
	}
	return "redis-server"
}

func fatal(err error) {
	showCursor()
	if colorTerm {
		fmt.Fprintf(os.Stderr, "\n  %s%serror:%s %v\n\n", ansiBold, ansiRed, ansiReset, err)
	} else {
		fmt.Fprintf(os.Stderr, "\n  error: %v\n\n", err)
	}
	os.Exit(1)
}
