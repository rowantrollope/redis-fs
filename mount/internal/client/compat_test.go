package client

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCompatBackendSmoke(t *testing.T) {
	t.Parallel()

	port := freeTCPPort(t)
	cmd := exec.Command(
		"redis-server",
		"--port", strconv.Itoa(port),
		"--save", "",
		"--appendonly", "no",
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:" + strconv.Itoa(port)})
	t.Cleanup(func() { _ = rdb.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("redis-server did not become ready")
		}
		time.Sleep(50 * time.Millisecond)
	}

	c := New(rdb, "compat-smoke")

	if err := c.Mkdir(ctx, "/a/b"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := c.Echo(ctx, "/a/b/file.txt", []byte("hello")); err != nil {
		t.Fatalf("echo: %v", err)
	}
	if err := c.EchoAppend(ctx, "/a/b/file.txt", []byte(" world")); err != nil {
		t.Fatalf("echo append: %v", err)
	}
	data, err := c.Cat(ctx, "/a/b/file.txt")
	if err != nil {
		t.Fatalf("cat: %v", err)
	}
	if string(data) != "hello world" {
		t.Fatalf("unexpected content: %q", string(data))
	}

	if err := c.Truncate(ctx, "/a/b/file.txt", 5); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	data, err = c.Cat(ctx, "/a/b/file.txt")
	if err != nil {
		t.Fatalf("cat after truncate: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected truncated content: %q", string(data))
	}

	if err := c.Ln(ctx, "../b/file.txt", "/a/link"); err != nil {
		t.Fatalf("ln: %v", err)
	}
	target, err := c.Readlink(ctx, "/a/link")
	if err != nil {
		t.Fatalf("readlink: %v", err)
	}
	if target != "../b/file.txt" {
		t.Fatalf("unexpected readlink target: %q", target)
	}

	if err := c.Mv(ctx, "/a/b/file.txt", "/a/b/file2.txt"); err != nil {
		t.Fatalf("mv: %v", err)
	}
	if _, err := c.Cat(ctx, "/a/b/file.txt"); err == nil {
		t.Fatal("expected old path to be missing after move")
	}
	if _, err := c.Cat(ctx, "/a/b/file2.txt"); err != nil {
		t.Fatalf("cat new path after move: %v", err)
	}

	entries, err := c.LsLong(ctx, "/a/b")
	if err != nil {
		t.Fatalf("ls long: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "file2.txt" {
		t.Fatalf("unexpected ls entries: %+v", entries)
	}

	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if info.Files < 1 || info.Directories < 1 {
		t.Fatalf("unexpected info: %+v", info)
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
