package qmd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps RedisSearch operations for redis-fs keys.
type Client struct {
	rdb       *redis.Client
	fsKey     string
	indexName string
	prefix    string
}

func NewClient(rdb *redis.Client, fsKey, indexName string) *Client {
	if indexName == "" {
		indexName = fmt.Sprintf("rfs_idx:{%s}", fsKey)
	}
	prefix := fmt.Sprintf("rfs:{%s}:inode:", fsKey)
	return &Client{rdb: rdb, fsKey: fsKey, indexName: indexName, prefix: prefix}
}

func (c *Client) IndexName() string { return c.indexName }

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) EnsurePathFields(ctx context.Context) (int, error) {
	var cursor uint64
	updated := 0
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, c.prefix+"*", 500).Result()
		if err != nil {
			return updated, err
		}
		for _, k := range keys {
			p := strings.TrimPrefix(k, c.prefix)
			if p == "" {
				continue
			}
			if err := c.rdb.HSet(ctx, k, "path", p).Err(); err != nil {
				return updated, err
			}
			updated++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return updated, nil
}

func (c *Client) IndexExists(ctx context.Context) (bool, error) {
	list, err := c.rdb.Do(ctx, "FT._LIST").Result()
	if err != nil {
		return false, err
	}
	arr, ok := list.([]interface{})
	if !ok {
		return false, nil
	}
	for _, it := range arr {
		if toString(it) == c.indexName {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) CreateIndex(ctx context.Context) error {
	_, err := c.EnsurePathFields(ctx)
	if err != nil {
		return err
	}
	args := []interface{}{
		"FT.CREATE", c.indexName,
		"ON", "HASH",
		"PREFIX", "1", c.prefix,
		"SCHEMA",
		"path", "TAG", "SORTABLE",
		"type", "TAG", "SORTABLE",
		"content", "TEXT",
		"size", "NUMERIC", "SORTABLE",
		"mtime_ms", "NUMERIC", "SORTABLE",
		"ctime_ms", "NUMERIC", "SORTABLE",
	}
	_, err = c.rdb.Do(ctx, args...).Result()
	if err != nil {
		errStr := strings.ToUpper(err.Error())
		if strings.Contains(errStr, "INDEX ALREADY EXISTS") {
			return nil
		}
	}
	return err
}

func (c *Client) RebuildIndex(ctx context.Context) error {
	_, _ = c.rdb.Do(ctx, "FT.DROPINDEX", c.indexName).Result()
	return c.CreateIndex(ctx)
}

func (c *Client) IndexInfo(ctx context.Context) (map[string]string, error) {
	res, err := c.rdb.Do(ctx, "FT.INFO", c.indexName).Result()
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok {
		return nil, errors.New("unexpected FT.INFO response")
	}
	out := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		out[toString(arr[i])] = toString(arr[i+1])
	}
	return out, nil
}

func (c *Client) Search(ctx context.Context, query string, opts QueryOptions) (int64, []SearchHit, error) {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	args := []interface{}{
		"FT.SEARCH", c.indexName, query,
		"WITHSCORES",
		"RETURN", "6", "path", "type", "content", "size", "mtime_ms", "ctime_ms",
		"LIMIT", opts.Offset, opts.Limit,
	}
	res, err := c.rdb.Do(ctx, args...).Result()
	if err != nil {
		return 0, nil, err
	}
	return parseSearchReply(res)
}

func (c *Client) Doctor(ctx context.Context) ([]string, error) {
	checks := []string{}
	if err := c.Ping(ctx); err != nil {
		return checks, err
	}
	checks = append(checks, "redis: ok")
	if _, err := c.rdb.Do(ctx, "FT._LIST").Result(); err != nil {
		return checks, fmt.Errorf("redisearch unavailable: %w", err)
	}
	checks = append(checks, "redisearch: ok")
	exists, err := c.IndexExists(ctx)
	if err != nil {
		return checks, err
	}
	if exists {
		checks = append(checks, "index: present")
	} else {
		checks = append(checks, "index: missing")
	}
	return checks, nil
}

func (c *Client) Watch(ctx context.Context, query string, opts QueryOptions, interval time.Duration, fn func(WatchEvent)) error {
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastSig string
	emit := func() error {
		_, hits, err := c.Search(ctx, query, opts)
		if err != nil {
			return err
		}
		sig := signature(hits)
		if sig != lastSig {
			lastSig = sig
			fn(WatchEvent{At: time.Now(), Hits: hits})
		}
		return nil
	}
	if err := emit(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

func (c *Client) Clean(ctx context.Context) (int, error) {
	// For HASH indexes RedisSearch usually drops deleted docs automatically.
	// We still probe top docs and remove any dangling IDs defensively.
	_, hits, err := c.Search(ctx, "*", QueryOptions{Limit: 10000, Offset: 0})
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, h := range hits {
		exists, err := c.rdb.Exists(ctx, h.DocID).Result()
		if err != nil {
			return removed, err
		}
		if exists == 0 {
			if _, err := c.rdb.Do(ctx, "FT.DEL", c.indexName, h.DocID, "DD").Result(); err == nil {
				removed++
			}
		}
	}
	return removed, nil
}

func RankedGrepLines(hits []SearchHit, needle string, nocase bool) []string {
	if nocase {
		needle = strings.ToLower(needle)
	}
	sorted := append([]SearchHit(nil), hits...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].Path < sorted[j].Path
		}
		return sorted[i].Score > sorted[j].Score
	})

	out := []string{}
	for _, h := range sorted {
		if h.Path == "" {
			continue
		}
		lines := strings.Split(h.Content, "\n")
		for i, line := range lines {
			testLine := line
			if nocase {
				testLine = strings.ToLower(testLine)
			}
			if needle == "" || strings.Contains(testLine, needle) {
				out = append(out, fmt.Sprintf("%s:%d:%s", h.Path, i+1, line))
			}
		}
	}
	return out
}

func parseSearchReply(res interface{}) (int64, []SearchHit, error) {
	arr, ok := res.([]interface{})
	if !ok || len(arr) == 0 {
		return 0, nil, errors.New("unexpected FT.SEARCH response")
	}
	total := toInt64(arr[0])
	hits := []SearchHit{}
	for i := 1; i+2 < len(arr); i += 3 {
		docID := toString(arr[i])
		score, _ := strconv.ParseFloat(toString(arr[i+1]), 64)
		fieldsArr, ok := arr[i+2].([]interface{})
		if !ok {
			continue
		}
		fields := map[string]string{}
		for j := 0; j+1 < len(fieldsArr); j += 2 {
			fields[toString(fieldsArr[j])] = toString(fieldsArr[j+1])
		}
		h := SearchHit{
			DocID:   docID,
			Path:    fields["path"],
			Type:    fields["type"],
			Content: fields["content"],
			Score:   score,
			Size:    toInt64(fields["size"]),
			MtimeMS: toInt64(fields["mtime_ms"]),
			CtimeMS: toInt64(fields["ctime_ms"]),
		}
		if h.Path == "" {
			idx := strings.Index(docID, ":inode:")
			if idx >= 0 {
				h.Path = docID[idx+len(":inode:"):]
			}
		}
		hits = append(hits, h)
	}
	return total, hits, nil
}

func signature(hits []SearchHit) string {
	parts := make([]string, 0, len(hits))
	for _, h := range hits {
		parts = append(parts, fmt.Sprintf("%s:%0.6f", h.DocID, h.Score))
	}
	return strings.Join(parts, "|")
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		if math.Trunc(t) == t {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case []byte:
		n, _ := strconv.ParseInt(string(t), 10, 64)
		return n
	default:
		return 0
	}
}
