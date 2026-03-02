package client

import (
	"context"
	"encoding/base64"
	"errors"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const maxCompatSymlinkDepth = 40

type compatClient struct {
	rdb  *redis.Client
	key  string
	keys compatKeys
}

type compatInode struct {
	Type    string
	Mode    uint32
	UID     uint32
	GID     uint32
	Size    int64
	CtimeMs int64
	MtimeMs int64
	AtimeMs int64
	Target  string
	Content string
}

func newCompatClient(rdb *redis.Client, key string) Client {
	return &compatClient{
		rdb:  rdb,
		key:  key,
		keys: newCompatKeys(key),
	}
}

func (c *compatClient) Stat(ctx context.Context, p string) (*StatResult, error) {
	resolved, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	_ = resolved
	return inode.toStat(), nil
}

func (c *compatClient) Cat(ctx context.Context, p string) ([]byte, error) {
	resolved, inode, err := c.resolvePath(ctx, p, true)
	if err != nil {
		return nil, err
	}
	if inode.Type != "file" {
		return nil, errors.New("not a file")
	}
	inode.AtimeMs = nowMs()
	if err := c.saveInode(ctx, resolved, inode); err != nil {
		return nil, err
	}
	return []byte(inode.Content), nil
}

func (c *compatClient) Echo(ctx context.Context, p string, data []byte) error {
	return c.writeFile(ctx, p, data, false)
}

func (c *compatClient) EchoAppend(ctx context.Context, p string, data []byte) error {
	return c.writeFile(ctx, p, data, true)
}

func (c *compatClient) Touch(ctx context.Context, p string) error {
	p = normalizeCompatPath(p)
	if p == "/" {
		return errors.New("cannot write to root")
	}
	if err := c.ensureParents(ctx, p); err != nil {
		return err
	}

	resolved, inode, err := c.resolvePath(ctx, p, true)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return c.createFile(ctx, p, "")
		}
		return err
	}
	if inode.Type != "file" {
		return errors.New("not a file")
	}
	now := nowMs()
	inode.MtimeMs = now
	inode.AtimeMs = now
	return c.saveInode(ctx, resolved, inode)
}

func (c *compatClient) Mkdir(ctx context.Context, p string) error {
	p = normalizeCompatPath(p)
	if p == "/" {
		return c.ensureRoot(ctx)
	}
	if err := c.ensureParents(ctx, p); err != nil {
		return err
	}
	existing, err := c.loadInode(ctx, p)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.Type == "dir" {
			return nil
		}
		return errors.New("already exists")
	}
	return c.createDir(ctx, p, 0o755)
}

func (c *compatClient) Rm(ctx context.Context, p string) error {
	p = normalizeCompatPath(p)
	if p == "/" {
		return errors.New("cannot remove root")
	}
	resolved, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		return err
	}
	if inode.Type == "dir" {
		n, err := c.rdb.SCard(ctx, c.keys.children(resolved)).Result()
		if err != nil {
			return err
		}
		if n > 0 {
			return errors.New("directory not empty")
		}
	}

	if err := c.rdb.Del(ctx, c.keys.inode(resolved)).Err(); err != nil {
		return err
	}
	if inode.Type == "dir" {
		if err := c.rdb.Del(ctx, c.keys.children(resolved)).Err(); err != nil {
			return err
		}
	}
	parent := parentOf(resolved)
	if parent != resolved {
		if err := c.rdb.SRem(ctx, c.keys.children(parent), baseName(resolved)).Err(); err != nil {
			return err
		}
	}
	return c.adjustInfoForDelete(ctx, inode)
}

func (c *compatClient) Ls(ctx context.Context, p string) ([]string, error) {
	entries, err := c.LsLong(ctx, p)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name)
	}
	return out, nil
}

func (c *compatClient) LsLong(ctx context.Context, p string) ([]LsEntry, error) {
	resolved, inode, err := c.resolvePath(ctx, p, true)
	if err != nil {
		return nil, err
	}
	if inode.Type != "dir" {
		return nil, errors.New("not a directory")
	}

	names, err := c.rdb.SMembers(ctx, c.keys.children(resolved)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	out := make([]LsEntry, 0, len(names))
	for _, name := range names {
		childPath := joinCompatPath(resolved, name)
		child, err := c.loadInode(ctx, childPath)
		if err != nil {
			return nil, err
		}
		if child == nil {
			continue
		}
		out = append(out, LsEntry{
			Name:  name,
			Type:  child.Type,
			Mode:  child.Mode,
			Size:  child.Size,
			Mtime: child.MtimeMs,
		})
	}
	return out, nil
}

func (c *compatClient) Mv(ctx context.Context, src, dst string) error {
	src = normalizeCompatPath(src)
	dst = normalizeCompatPath(dst)
	if src == "/" {
		return errors.New("cannot move root")
	}
	if src == dst {
		return nil
	}

	if _, _, err := c.resolvePath(ctx, dst, false); err == nil {
		return errors.New("already exists")
	} else if !errors.Is(err, redis.Nil) {
		return err
	}
	resolvedSrc, srcInode, err := c.resolvePath(ctx, src, false)
	if err != nil {
		return err
	}
	if srcInode.Type == "dir" && (dst == resolvedSrc || strings.HasPrefix(dst, resolvedSrc+"/")) {
		return errors.New("cannot move a directory into its own subtree")
	}
	if err := c.ensureParents(ctx, dst); err != nil {
		return err
	}

	paths, err := c.listAllInodePaths(ctx)
	if err != nil {
		return err
	}
	moved := make([]string, 0)
	for _, p := range paths {
		if p == resolvedSrc || strings.HasPrefix(p, resolvedSrc+"/") {
			moved = append(moved, p)
		}
	}
	sort.Slice(moved, func(i, j int) bool { return len(moved[i]) < len(moved[j]) })

	for _, oldPath := range moved {
		newPath := dst + strings.TrimPrefix(oldPath, resolvedSrc)
		values, err := c.rdb.HGetAll(ctx, c.keys.inode(oldPath)).Result()
		if err != nil {
			return err
		}
		if len(values) == 0 {
			continue
		}
		if err := c.rdb.HSet(ctx, c.keys.inode(newPath), values).Err(); err != nil {
			return err
		}
		if values["type"] == "dir" {
			children, err := c.rdb.SMembers(ctx, c.keys.children(oldPath)).Result()
			if err != nil {
				return err
			}
			if len(children) > 0 {
				args := make([]interface{}, 0, len(children))
				for _, child := range children {
					args = append(args, child)
				}
				if err := c.rdb.SAdd(ctx, c.keys.children(newPath), args...).Err(); err != nil {
					return err
				}
			}
		}
	}
	for i := len(moved) - 1; i >= 0; i-- {
		oldPath := moved[i]
		if err := c.rdb.Del(ctx, c.keys.inode(oldPath), c.keys.children(oldPath)).Err(); err != nil {
			return err
		}
	}

	oldParent := parentOf(resolvedSrc)
	newParent := parentOf(dst)
	if err := c.rdb.SRem(ctx, c.keys.children(oldParent), baseName(resolvedSrc)).Err(); err != nil {
		return err
	}
	if err := c.rdb.SAdd(ctx, c.keys.children(newParent), baseName(dst)).Err(); err != nil {
		return err
	}
	return nil
}

func (c *compatClient) Ln(ctx context.Context, target, linkpath string) error {
	linkpath = normalizeCompatPath(linkpath)
	if linkpath == "/" {
		return errors.New("already exists")
	}
	if err := c.ensureParents(ctx, linkpath); err != nil {
		return err
	}
	existing, err := c.loadInode(ctx, linkpath)
	if err != nil {
		return err
	}
	if existing != nil {
		return errors.New("already exists")
	}
	now := nowMs()
	inode := &compatInode{
		Type:    "symlink",
		Mode:    0o777,
		UID:     0,
		GID:     0,
		Size:    int64(len(target)),
		CtimeMs: now,
		MtimeMs: now,
		AtimeMs: now,
		Target:  target,
	}
	if err := c.saveInode(ctx, linkpath, inode); err != nil {
		return err
	}
	if err := c.rdb.SAdd(ctx, c.keys.children(parentOf(linkpath)), baseName(linkpath)).Err(); err != nil {
		return err
	}
	return c.adjustInfoForCreate(ctx, inode)
}

func (c *compatClient) Readlink(ctx context.Context, p string) (string, error) {
	_, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		return "", err
	}
	if inode.Type != "symlink" {
		return "", errors.New("not a symlink")
	}
	return inode.Target, nil
}

func (c *compatClient) Chmod(ctx context.Context, p string, mode uint32) error {
	resolved, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		return err
	}
	inode.Mode = mode
	return c.saveInode(ctx, resolved, inode)
}

func (c *compatClient) Chown(ctx context.Context, p string, uid, gid uint32) error {
	resolved, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		return err
	}
	inode.UID = uid
	inode.GID = gid
	return c.saveInode(ctx, resolved, inode)
}

func (c *compatClient) Truncate(ctx context.Context, p string, size int64) error {
	if size < 0 {
		return errors.New("invalid size")
	}
	resolved, inode, err := c.resolvePath(ctx, p, true)
	if err != nil {
		return err
	}
	if inode.Type != "file" {
		return errors.New("not a file")
	}
	content := []byte(inode.Content)
	if int64(len(content)) > size {
		content = content[:size]
	} else if int64(len(content)) < size {
		newBuf := make([]byte, size)
		copy(newBuf, content)
		content = newBuf
	}
	delta := int64(len(content)) - inode.Size
	inode.Content = string(content)
	inode.Size = int64(len(content))
	now := nowMs()
	inode.MtimeMs = now
	inode.AtimeMs = now
	if err := c.saveInode(ctx, resolved, inode); err != nil {
		return err
	}
	return c.adjustTotalData(ctx, delta)
}

func (c *compatClient) Utimens(ctx context.Context, p string, atimeMs, mtimeMs int64) error {
	resolved, inode, err := c.resolvePath(ctx, p, false)
	if err != nil {
		return err
	}
	if atimeMs >= 0 {
		inode.AtimeMs = atimeMs
	}
	if mtimeMs >= 0 {
		inode.MtimeMs = mtimeMs
	}
	return c.saveInode(ctx, resolved, inode)
}

func (c *compatClient) Info(ctx context.Context) (*InfoResult, error) {
	values, err := c.rdb.HGetAll(ctx, c.keys.info()).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return &InfoResult{}, nil
	}
	files := parseInt64OrZero(values["files"])
	dirs := parseInt64OrZero(values["directories"])
	symlinks := parseInt64OrZero(values["symlinks"])
	totalData := parseInt64OrZero(values["total_data_bytes"])
	return &InfoResult{
		Files:          files,
		Directories:    dirs,
		Symlinks:       symlinks,
		TotalDataBytes: totalData,
		TotalInodes:    files + dirs + symlinks,
	}, nil
}

func (c *compatClient) writeFile(ctx context.Context, p string, data []byte, appendMode bool) error {
	p = normalizeCompatPath(p)
	if p == "/" {
		return errors.New("cannot write to root")
	}
	if err := c.ensureParents(ctx, p); err != nil {
		return err
	}
	resolved, inode, err := c.resolvePath(ctx, p, true)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return c.createFile(ctx, p, string(data))
		}
		return err
	}
	if inode.Type != "file" {
		return errors.New("not a file")
	}
	before := inode.Size
	if appendMode {
		inode.Content += string(data)
	} else {
		inode.Content = string(data)
	}
	inode.Size = int64(len(inode.Content))
	now := nowMs()
	inode.MtimeMs = now
	inode.AtimeMs = now
	if err := c.saveInode(ctx, resolved, inode); err != nil {
		return err
	}
	return c.adjustTotalData(ctx, inode.Size-before)
}

func (c *compatClient) createFile(ctx context.Context, p string, content string) error {
	if err := c.ensureParents(ctx, p); err != nil {
		return err
	}
	now := nowMs()
	inode := &compatInode{
		Type:    "file",
		Mode:    0o644,
		UID:     0,
		GID:     0,
		Size:    int64(len(content)),
		CtimeMs: now,
		MtimeMs: now,
		AtimeMs: now,
		Content: content,
	}
	if err := c.saveInode(ctx, p, inode); err != nil {
		return err
	}
	if err := c.rdb.SAdd(ctx, c.keys.children(parentOf(p)), baseName(p)).Err(); err != nil {
		return err
	}
	return c.adjustInfoForCreate(ctx, inode)
}

func (c *compatClient) createDir(ctx context.Context, p string, mode uint32) error {
	if err := c.ensureParents(ctx, p); err != nil {
		return err
	}
	return c.createDirNoParents(ctx, p, mode)
}

func (c *compatClient) createDirNoParents(ctx context.Context, p string, mode uint32) error {
	now := nowMs()
	inode := &compatInode{
		Type:    "dir",
		Mode:    mode,
		UID:     0,
		GID:     0,
		Size:    0,
		CtimeMs: now,
		MtimeMs: now,
		AtimeMs: now,
	}
	if err := c.saveInode(ctx, p, inode); err != nil {
		return err
	}
	if err := c.rdb.SAdd(ctx, c.keys.children(parentOf(p)), baseName(p)).Err(); err != nil {
		return err
	}
	return c.adjustInfoForCreate(ctx, inode)
}

func (c *compatClient) ensureRoot(ctx context.Context) error {
	root, err := c.loadInode(ctx, "/")
	if err != nil {
		return err
	}
	if root != nil {
		return nil
	}
	now := nowMs()
	root = &compatInode{
		Type:    "dir",
		Mode:    0o755,
		UID:     0,
		GID:     0,
		Size:    0,
		CtimeMs: now,
		MtimeMs: now,
		AtimeMs: now,
	}
	if err := c.saveInode(ctx, "/", root); err != nil {
		return err
	}
	return c.rdb.HSet(ctx, c.keys.info(), map[string]interface{}{
		"schema_version":   "1",
		"files":            0,
		"directories":      1,
		"symlinks":         0,
		"total_data_bytes": 0,
	}).Err()
}

func (c *compatClient) ensureParents(ctx context.Context, p string) error {
	if err := c.ensureRoot(ctx); err != nil {
		return err
	}
	parent := parentOf(p)
	if parent == "/" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(parent, "/"), "/")
	cur := "/"
	for _, part := range parts {
		cur = joinCompatPath(cur, part)
		inode, err := c.loadInode(ctx, cur)
		if err != nil {
			return err
		}
		if inode != nil {
			if inode.Type != "dir" {
				return errors.New("parent path conflict")
			}
			continue
		}
		if err := c.createDirNoParents(ctx, cur, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func (c *compatClient) resolvePath(ctx context.Context, p string, followFinal bool) (string, *compatInode, error) {
	p = normalizeCompatPath(p)
	components := splitComponents(p)
	cur := "/"
	depth := 0
	for i := 0; ; {
		next := cur
		if i < len(components) {
			next = joinCompatPath(cur, components[i])
		}
		inode, err := c.loadInode(ctx, next)
		if err != nil {
			return "", nil, err
		}
		if inode == nil {
			return "", nil, redis.Nil
		}
		isFinal := i == len(components)-1
		if inode.Type == "symlink" && (followFinal || !isFinal) {
			depth++
			if depth > maxCompatSymlinkDepth {
				return "", nil, errors.New("too many levels of symbolic links")
			}
			remaining := ""
			if i+1 < len(components) {
				remaining = strings.Join(components[i+1:], "/")
			}
			target := inode.Target
			if target == "" {
				return "", nil, errors.New("invalid symlink target")
			}
			var rebuilt string
			if strings.HasPrefix(target, "/") {
				rebuilt = target
			} else {
				rebuilt = path.Join(cur, target)
			}
			if remaining != "" {
				rebuilt = path.Join(rebuilt, remaining)
			}
			p = normalizeCompatPath(rebuilt)
			components = splitComponents(p)
			cur = "/"
			i = 0
			continue
		}
		if i >= len(components)-1 {
			return next, inode, nil
		}
		if inode.Type != "dir" {
			return "", nil, errors.New("not a directory")
		}
		cur = next
		i++
	}
}

func (c *compatClient) loadInode(ctx context.Context, p string) (*compatInode, error) {
	values, err := c.rdb.HGetAll(ctx, c.keys.inode(p)).Result()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	inode := &compatInode{
		Type:    values["type"],
		Mode:    uint32(parseInt64OrZero(values["mode"])),
		UID:     uint32(parseInt64OrZero(values["uid"])),
		GID:     uint32(parseInt64OrZero(values["gid"])),
		Size:    parseInt64OrZero(values["size"]),
		CtimeMs: parseInt64OrZero(values["ctime_ms"]),
		MtimeMs: parseInt64OrZero(values["mtime_ms"]),
		AtimeMs: parseInt64OrZero(values["atime_ms"]),
		Target:  values["target"],
		Content: values["content"],
	}
	return inode, nil
}

func (c *compatClient) saveInode(ctx context.Context, p string, inode *compatInode) error {
	fields := map[string]interface{}{
		"type":     inode.Type,
		"mode":     inode.Mode,
		"uid":      inode.UID,
		"gid":      inode.GID,
		"size":     inode.Size,
		"ctime_ms": inode.CtimeMs,
		"mtime_ms": inode.MtimeMs,
		"atime_ms": inode.AtimeMs,
	}
	if inode.Type == "symlink" {
		fields["target"] = inode.Target
	}
	if inode.Type == "file" {
		fields["content"] = inode.Content
	}
	return c.rdb.HSet(ctx, c.keys.inode(p), fields).Err()
}

func (c *compatClient) adjustInfoForCreate(ctx context.Context, inode *compatInode) error {
	if err := c.ensureRoot(ctx); err != nil {
		return err
	}
	switch inode.Type {
	case "file":
		if err := c.rdb.HIncrBy(ctx, c.keys.info(), "files", 1).Err(); err != nil {
			return err
		}
		return c.adjustTotalData(ctx, inode.Size)
	case "dir":
		return c.rdb.HIncrBy(ctx, c.keys.info(), "directories", 1).Err()
	case "symlink":
		return c.rdb.HIncrBy(ctx, c.keys.info(), "symlinks", 1).Err()
	default:
		return nil
	}
}

func (c *compatClient) adjustInfoForDelete(ctx context.Context, inode *compatInode) error {
	if err := c.ensureRoot(ctx); err != nil {
		return err
	}
	switch inode.Type {
	case "file":
		if err := c.rdb.HIncrBy(ctx, c.keys.info(), "files", -1).Err(); err != nil {
			return err
		}
		return c.adjustTotalData(ctx, -inode.Size)
	case "dir":
		return c.rdb.HIncrBy(ctx, c.keys.info(), "directories", -1).Err()
	case "symlink":
		return c.rdb.HIncrBy(ctx, c.keys.info(), "symlinks", -1).Err()
	default:
		return nil
	}
}

func (c *compatClient) adjustTotalData(ctx context.Context, delta int64) error {
	if delta == 0 {
		return nil
	}
	return c.rdb.HIncrBy(ctx, c.keys.info(), "total_data_bytes", delta).Err()
}

func (c *compatClient) listAllInodePaths(ctx context.Context) ([]string, error) {
	var (
		cursor uint64
		paths  []string
	)
	prefix := c.keys.inodePrefix()
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, prefix+"*", 200).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			encoded := strings.TrimPrefix(key, prefix)
			b, err := decodeCompatPath(encoded)
			if err != nil {
				continue
			}
			paths = append(paths, b)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return paths, nil
}

func (i *compatInode) toStat() *StatResult {
	return &StatResult{
		Type:  i.Type,
		Mode:  i.Mode,
		UID:   i.UID,
		GID:   i.GID,
		Size:  i.Size,
		Ctime: i.CtimeMs,
		Mtime: i.MtimeMs,
		Atime: i.AtimeMs,
	}
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}

func splitComponents(p string) []string {
	if p == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(p, "/"), "/")
}

func joinCompatPath(parent, child string) string {
	if parent == "/" {
		return "/" + child
	}
	return parent + "/" + child
}

func parentOf(p string) string {
	if p == "/" {
		return "/"
	}
	parent := path.Dir(p)
	if parent == "." {
		return "/"
	}
	return parent
}

func baseName(p string) string {
	if p == "/" {
		return ""
	}
	return path.Base(p)
}

func decodeCompatPath(s string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseInt64OrZero(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
