package client

import (
	internal "github.com/redis-fs/mount/internal/client"
	"github.com/redis/go-redis/v9"
)

type Client = internal.Client
type StatResult = internal.StatResult
type LsEntry = internal.LsEntry
type InfoResult = internal.InfoResult

func New(rdb *redis.Client, key string) Client {
	return internal.New(rdb, key)
}
