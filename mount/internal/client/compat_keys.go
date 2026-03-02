package client

import (
	"encoding/base64"
	"path"
	"strings"
)

type compatKeys struct {
	fsKey string
}

func newCompatKeys(fsKey string) compatKeys {
	return compatKeys{fsKey: fsKey}
}

func (k compatKeys) inode(p string) string {
	return "rfs:compat:" + k.fsKey + ":inode:" + encodeCompatPath(p)
}

func (k compatKeys) children(p string) string {
	return "rfs:compat:" + k.fsKey + ":children:" + encodeCompatPath(p)
}

func (k compatKeys) info() string {
	return "rfs:compat:" + k.fsKey + ":info"
}

func (k compatKeys) inodePrefix() string {
	return "rfs:compat:" + k.fsKey + ":inode:"
}

func (k compatKeys) childrenPrefix() string {
	return "rfs:compat:" + k.fsKey + ":children:"
}

func encodeCompatPath(p string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(p))
}

func normalizeCompatPath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	clean := path.Clean(p)
	if clean == "." {
		return "/"
	}
	return clean
}
