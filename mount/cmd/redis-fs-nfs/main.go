package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-git/go-billy/v5"
	"github.com/redis-fs/mount/internal/client"
	"github.com/redis-fs/mount/internal/nfsfs"
	"github.com/redis/go-redis/v9"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
)

type authCompatHandler struct {
	nfs.Handler
}

func (h authCompatHandler) Mount(ctx context.Context, conn net.Conn, req nfs.MountRequest) (nfs.MountStatus, billy.Filesystem, []nfs.AuthFlavor) {
	status, fs, flavors := h.Handler.Mount(ctx, conn, req)
	if status != nfs.MountStatusOk {
		return status, fs, flavors
	}

	hasNull := false
	hasUnix := false
	for _, fl := range flavors {
		if fl == nfs.AuthFlavorNull {
			hasNull = true
		}
		if fl == nfs.AuthFlavorUnix {
			hasUnix = true
		}
	}
	if !hasUnix {
		flavors = append(flavors, nfs.AuthFlavorUnix)
	}
	if !hasNull {
		flavors = append(flavors, nfs.AuthFlavorNull)
	}
	return status, fs, flavors
}

func main() {
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	redisPassword := flag.String("password", "", "Redis password")
	redisDB := flag.Int("db", 0, "Redis database number")
	listenAddr := flag.String("listen", "127.0.0.1:20490", "Listen address for NFS server")
	exportPath := flag.String("export", "/myfs", "Exported NFS path")
	readOnly := flag.Bool("readonly", false, "Export read-only")
	foreground := flag.Bool("foreground", true, "Run in foreground")
	flag.Parse()

	if !*foreground {
		log.Printf("--foreground=false is not supported; running foreground")
	}

	exp := strings.TrimSpace(*exportPath)
	if exp == "" || !strings.HasPrefix(exp, "/") {
		log.Fatalf("invalid --export %q: expected absolute path", *exportPath)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     *redisAddr,
		Password: *redisPassword,
		DB:       *redisDB,
		PoolSize: 16,
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to Redis at %s: %v", *redisAddr, err)
	}

	redisKey := strings.TrimPrefix(exp, "/")
	if redisKey == "" {
		redisKey = "myfs"
	}
	c := client.New(rdb, redisKey)
	if err := c.Touch(ctx, "/.nfs-check"); err != nil {
		log.Fatalf("failed to initialize key %q: %v", redisKey, err)
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen failed on %s: %v", *listenAddr, err)
	}
	defer listener.Close()

	fs := nfsfs.New(c, *readOnly)
	baseHandler := helpers.NewNullAuthHandler(fs)
	// go-nfs expects stable non-empty file handles for client mount handshakes.
	// Wrapping with CachingHandler provides ToHandle/FromHandle mapping.
	handler := authCompatHandler{Handler: helpers.NewCachingHandler(baseHandler, 1024)}

	log.Printf("Serving Redis key %q via NFS at %s", redisKey, *listenAddr)
	log.Printf("Export path: %s", exp)
	log.Printf("Mount target example: %s:%s", hostOnly(*listenAddr), exp)

	errCh := make(chan error, 1)
	go func() {
		errCh <- nfs.Serve(listener, handler)
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("Received signal %v, shutting down", sig)
		_ = listener.Close()
	case err := <-errCh:
		if err != nil {
			log.Fatalf("nfs server failed: %v", err)
		}
	}
}

func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1"
	}
	if host == "" || host == "0.0.0.0" {
		return "127.0.0.1"
	}
	if host == "::" {
		return "::1"
	}
	return host
}
