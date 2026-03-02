// Package client provides filesystem client backends over Redis.
package client

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// Client provides the filesystem operation surface used by the mount layer.
type Client interface {
	Stat(ctx context.Context, path string) (*StatResult, error)
	Cat(ctx context.Context, path string) ([]byte, error)
	Echo(ctx context.Context, path string, data []byte) error
	EchoAppend(ctx context.Context, path string, data []byte) error
	Touch(ctx context.Context, path string) error
	Mkdir(ctx context.Context, path string) error
	Rm(ctx context.Context, path string) error
	Ls(ctx context.Context, path string) ([]string, error)
	LsLong(ctx context.Context, path string) ([]LsEntry, error)
	Mv(ctx context.Context, src, dst string) error
	Ln(ctx context.Context, target, linkpath string) error
	Readlink(ctx context.Context, path string) (string, error)
	Chmod(ctx context.Context, path string, mode uint32) error
	Chown(ctx context.Context, path string, uid, gid uint32) error
	Truncate(ctx context.Context, path string, size int64) error
	Utimens(ctx context.Context, path string, atimeMs, mtimeMs int64) error
	Info(ctx context.Context) (*InfoResult, error)
}

type moduleClient struct {
	rdb *redis.Client
	key string // Redis key holding the filesystem
}

// New creates a filesystem client for the given Redis key.
// It auto-selects the FS module backend when available, otherwise compatibility mode.
func New(rdb *redis.Client, key string) Client {
	if hasFSModule(context.Background(), rdb) {
		return &moduleClient{rdb: rdb, key: key}
	}
	log.Printf("Redis FS module not detected; using compatibility backend")
	return newCompatClient(rdb, key)
}

// Stat returns metadata for a path. Returns nil, nil if path does not exist.
func (c *moduleClient) Stat(ctx context.Context, path string) (*StatResult, error) {
	res, err := c.rdb.Do(ctx, "FS.STAT", c.key, path).Slice()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // path does not exist
		}
		return nil, err
	}
	return parseStat(res)
}

// Cat returns the file content at path.
func (c *moduleClient) Cat(ctx context.Context, path string) ([]byte, error) {
	val, err := c.rdb.Do(ctx, "FS.CAT", c.key, path).Result()
	if err != nil {
		return nil, err
	}
	switch v := val.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected CAT response type: %T", val)
	}
}

// Echo writes content to a file (creates or overwrites).
func (c *moduleClient) Echo(ctx context.Context, path string, data []byte) error {
	return c.rdb.Do(ctx, "FS.ECHO", c.key, path, data).Err()
}

// EchoAppend appends content to a file.
func (c *moduleClient) EchoAppend(ctx context.Context, path string, data []byte) error {
	return c.rdb.Do(ctx, "FS.ECHO", c.key, path, data, "APPEND").Err()
}

// Touch creates an empty file.
func (c *moduleClient) Touch(ctx context.Context, path string) error {
	return c.rdb.Do(ctx, "FS.TOUCH", c.key, path).Err()
}

// Mkdir creates a directory (with PARENTS to auto-create ancestors).
func (c *moduleClient) Mkdir(ctx context.Context, path string) error {
	return c.rdb.Do(ctx, "FS.MKDIR", c.key, path, "PARENTS").Err()
}

// Rm removes a file, directory, or symlink.
func (c *moduleClient) Rm(ctx context.Context, path string) error {
	return c.rdb.Do(ctx, "FS.RM", c.key, path).Err()
}

// Ls returns the children of a directory.
func (c *moduleClient) Ls(ctx context.Context, path string) ([]string, error) {
	return c.rdb.Do(ctx, "FS.LS", c.key, path).StringSlice()
}

// LsLong returns detailed directory listing.
func (c *moduleClient) LsLong(ctx context.Context, path string) ([]LsEntry, error) {
	res, err := c.rdb.Do(ctx, "FS.LS", c.key, path, "LONG").Slice()
	if err != nil {
		return nil, err
	}
	return parseLsLong(res)
}

// Mv renames/moves a path.
func (c *moduleClient) Mv(ctx context.Context, src, dst string) error {
	return c.rdb.Do(ctx, "FS.MV", c.key, src, dst).Err()
}

// Ln creates a symbolic link.
func (c *moduleClient) Ln(ctx context.Context, target, linkpath string) error {
	return c.rdb.Do(ctx, "FS.LN", c.key, target, linkpath).Err()
}

// Readlink returns the target of a symbolic link.
func (c *moduleClient) Readlink(ctx context.Context, path string) (string, error) {
	return c.rdb.Do(ctx, "FS.READLINK", c.key, path).Text()
}

// Chmod changes file permissions.
func (c *moduleClient) Chmod(ctx context.Context, path string, mode uint32) error {
	modeStr := fmt.Sprintf("%04o", mode)
	return c.rdb.Do(ctx, "FS.CHMOD", c.key, path, modeStr).Err()
}

// Chown changes file owner and group.
func (c *moduleClient) Chown(ctx context.Context, path string, uid, gid uint32) error {
	return c.rdb.Do(ctx, "FS.CHOWN", c.key, path, uid, gid).Err()
}

// Truncate truncates or extends a file to the given length.
func (c *moduleClient) Truncate(ctx context.Context, path string, size int64) error {
	return c.rdb.Do(ctx, "FS.TRUNCATE", c.key, path, size).Err()
}

// Utimens sets access and modification times (milliseconds). -1 means don't change.
func (c *moduleClient) Utimens(ctx context.Context, path string, atimeMs, mtimeMs int64) error {
	return c.rdb.Do(ctx, "FS.UTIMENS", c.key, path, atimeMs, mtimeMs).Err()
}

// Info returns filesystem-level statistics.
func (c *moduleClient) Info(ctx context.Context) (*InfoResult, error) {
	res, err := c.rdb.Do(ctx, "FS.INFO", c.key).Slice()
	if err != nil {
		return nil, err
	}
	return parseInfo(res)
}

func hasFSModule(ctx context.Context, rdb *redis.Client) bool {
	res, err := rdb.Do(ctx, "COMMAND", "LIST", "FILTERBY", "MODULE", "fs").Slice()
	return err == nil && len(res) > 0
}
