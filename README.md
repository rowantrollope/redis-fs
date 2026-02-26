# redis-fs

A native Redis module that implements a POSIX-like virtual filesystem as a custom data type with an `FS.*` command family.

**One Redis key = one filesystem.** Internally a flat hashmap of absolute paths to inodes — each storing type (file/dir/symlink), POSIX metadata, and inline content. Think `RedisJSON` but for filesystems.

Commands are named after their Unix counterparts: `FS.CAT`, `FS.LS`, `FS.MKDIR`, `FS.GREP`, etc.

## Building

```bash
make
```

## Loading

```bash
redis-server --loadmodule ./fs.so
```

## Quick Start

```
> FS.ECHO myfs /hello.txt "Hello, World!"
OK
> FS.CAT myfs /hello.txt
"Hello, World!"
> FS.MKDIR myfs /docs/notes PARENTS
OK
> FS.LS myfs /
1) "hello.txt"
2) "docs"
> FS.TREE myfs /
1) "/"
2) 1) "hello.txt"
   2) 1) "docs/"
      2) 1) "notes/"
> FS.FIND myfs / "*.txt"
1) "/hello.txt"
> FS.GREP myfs / "*Hello*"
1) 1) "/hello.txt"
   2) (integer) 1
   3) "Hello, World!"
```

## Commands

| Command | Description |
|---------|-------------|
| `FS.INFO key` | Filesystem stats (file/dir/symlink counts, total bytes) |
| `FS.ECHO key path content` | Write a file (auto-creates parents) |
| `FS.CAT key path` | Read file content (follows symlinks) |
| `FS.APPEND key path content` | Append to a file |
| `FS.RM key path [RECURSIVE]` | Delete file or directory |
| `FS.TOUCH key path` | Create empty file or update timestamps |
| `FS.MKDIR key path [PARENTS]` | Create directory |
| `FS.LS key path [LONG]` | List directory contents |
| `FS.STAT key path` | Get inode metadata |
| `FS.TEST key path` | Check if path exists |
| `FS.CHMOD key path mode` | Change permission bits |
| `FS.CHOWN key path uid [gid]` | Change ownership |
| `FS.LN key target linkpath` | Create symbolic link |
| `FS.READLINK key path` | Read symlink target |
| `FS.CP key src dst [RECURSIVE]` | Copy file or directory |
| `FS.MV key src dst` | Move / rename |
| `FS.TREE key path [DEPTH n]` | Recursive tree view |
| `FS.FIND key path pattern [TYPE t]` | Find by glob pattern |
| `FS.GREP key path pattern [NOCASE]` | Search file contents |

## Documentation

Full documentation — command reference with examples, Unix-to-Redis mapping table, data model, persistence, performance notes — is in **[docs.md](docs.md)**.

## License

BSD-2-Clause
