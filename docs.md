This module implements a virtual filesystem for Redis, a new data type
that stores a complete POSIX-like filesystem inside a single Redis key.
The fundamental goal is to provide filesystem operations — create files,
read them, organize them into directories, search their contents — all
as atomic Redis commands, without the overhead of managing dozens of
scattered keys per volume.

Think of it as RedisJSON for filesystems. Where JSON.* gives you a
document, FS.* gives you a directory tree with files, symlinks,
permissions, and all the metadata you'd expect from a real filesystem.

The commands are named after their Unix counterparts. If you know
`cat`, `ls`, `mkdir`, `grep` — you already know this module.

Each filesystem lives under one key. Multiple filesystems are just
multiple keys. No key prefix schemes, no naming conventions, no
cleanup scripts. `DEL myfs` and the whole filesystem is gone.

## Installation

Build with:

    make

Then load the module:

    redis-server --loadmodule ./fs.so

Or if you're already running Redis with module loading enabled:

    MODULE LOAD /path/to/fs.so

## Quick start

    > FS.ECHO myfs /hello.txt "Hello, World!"
    OK

    > FS.CAT myfs /hello.txt
    "Hello, World!"

    > FS.MKDIR myfs /etc/nginx PARENTS
    OK

    > FS.ECHO myfs /etc/nginx/nginx.conf "worker_processes auto;"
    OK

    > FS.LS myfs /
    1) "hello.txt"
    2) "etc"

    > FS.TREE myfs /
    1) "/"
    2) 1) "hello.txt"
       2) 1) "etc/"
          2) 1) 1) "nginx/"
                2) 1) "nginx.conf"

    > FS.FIND myfs / "*.conf"
    1) "/etc/nginx/nginx.conf"

    > FS.GREP myfs / "*worker*"
    1) 1) "/etc/nginx/nginx.conf"
       2) (integer) 1
       3) "worker_processes auto;"

    > FS.INFO myfs
     1) "files"
     2) (integer) 2
     3) "directories"
     4) (integer) 3
     5) "symlinks"
     6) (integer) 0
     7) "total_data_bytes"
     8) (integer) 35
     9) "total_inodes"
    10) (integer) 5

## Unix to Redis command mapping

If you've used a terminal, you already know how to use this module.
The commands are the same — just prefixed with `FS.` and taking a
key as the first argument. We dropped concepts that don't make sense
server-side (like `cd` and `pwd`, which are stateful client concerns)
and merged `rm` and `rmdir` into one command.

| Unix command | Redis command | Notes |
|---|---|---|
| `cat file` | `FS.CAT key /file` | Follows symlinks |
| `echo "text" > file` | `FS.ECHO key /file "text"` | Creates parents automatically |
| `echo "text" >> file` | `FS.APPEND key /file "text"` | Creates file if missing |
| `touch file` | `FS.TOUCH key /file` | Creates or updates mtime |
| `rm file` | `FS.RM key /file` | Works on files, dirs, symlinks |
| `rm -r dir` | `FS.RM key /dir RECURSIVE` | Deletes entire subtree |
| `rmdir dir` | `FS.RM key /dir` | Fails if not empty (use RECURSIVE) |
| `mkdir dir` | `FS.MKDIR key /dir` | Parent must exist |
| `mkdir -p a/b/c` | `FS.MKDIR key /a/b/c PARENTS` | Creates intermediates |
| `ls dir` | `FS.LS key /dir` | Returns child names |
| `ls -l dir` | `FS.LS key /dir LONG` | Includes type, mode, size, mtime |
| `stat file` | `FS.STAT key /file` | Full metadata: type, mode, uid, gid, times |
| `test -e file` | `FS.TEST key /file` | Returns 1 or 0 |
| `chmod 0755 file` | `FS.CHMOD key /file 0755` | Octal mode string |
| `chown uid:gid file` | `FS.CHOWN key /file uid gid` | Separate uid and gid args |
| `ln -s target link` | `FS.LN key /target /link` | Target can be relative or absolute |
| `readlink link` | `FS.READLINK key /link` | Returns raw target string |
| `cp src dst` | `FS.CP key /src /dst` | Files only without RECURSIVE |
| `cp -r src dst` | `FS.CP key /src /dst RECURSIVE` | Deep copy with metadata |
| `mv src dst` | `FS.MV key /src /dst` | Moves entire subtrees atomically |
| `tree dir` | `FS.TREE key /dir` | Nested array structure |
| `tree -L 2 dir` | `FS.TREE key /dir DEPTH 2` | Limits recursion depth |
| `find dir -name "*.txt"` | `FS.FIND key /dir "*.txt"` | Glob matching on basenames |
| `find dir -name "*.txt" -type f` | `FS.FIND key /dir "*.txt" TYPE file` | Filter by type |
| `grep -r "pattern" dir` | `FS.GREP key /dir "*pattern*"` | Glob match on each line |
| `grep -ri "pattern" dir` | `FS.GREP key /dir "*pattern*" NOCASE` | Case-insensitive |
| `df` / `du` | `FS.INFO key` | File/dir/symlink counts + total bytes |
| `cd`, `pwd` | *(not applicable)* | Client-side state, not server commands |
| `vol list`, `vol switch` | *(not applicable)* | Volumes are just keys: use `KEYS`, `SCAN` |

## Data model

The filesystem is stored as a custom Redis data type. Internally it's a
flat dictionary mapping absolute paths (like `/etc/nginx/nginx.conf`) to
inodes. Each inode stores:

- **Type**: file, directory, or symbolic link
- **Mode**: POSIX permission bits (e.g., 0755)
- **Owner**: uid and gid, both uint32
- **Timestamps**: ctime, mtime, atime in milliseconds since epoch
- **Payload**: file content (inline bytes), directory children (name array), or symlink target (string)

File content is stored inline in the inode. There's no chunking and no
separate data key — Redis handles large allocations just fine. A 10 MB
file is a 10 MB allocation inside the inode. This keeps the
implementation simple and atomic: an `FS.ECHO` is a single dict
lookup and a memory copy, not a multi-key transaction.

Directories store an array of child *names* (not full paths). When you
call `FS.LS`, we return that array directly. When you call `FS.TREE`,
we walk the tree by joining child names to the current path and looking
them up in the dict.

Paths are always normalized to absolute form. Leading `./` and `../`
components are resolved. Multiple slashes collapse. Trailing slashes
are stripped. The root `/` is created automatically when the first
write operation touches a key.

## Key lifecycle

Filesystem keys follow the standard Redis convention: **the first write
creates the key, and deleting the last entry removes it**.

There is no `MKFS` or `INIT` command. When you run `FS.ECHO`, `FS.MKDIR`,
`FS.TOUCH`, or any other write command against a key that doesn't exist,
the module creates the key with an empty root directory, then performs
the operation. This is identical to how `SADD` creates a set on first
add, or `HSET` creates a hash on first field.

    > EXISTS myfs
    (integer) 0
    > FS.ECHO myfs /hello.txt "world"
    OK
    > EXISTS myfs
    (integer) 1

When the last file or directory is removed (leaving only the root),
the key is automatically deleted:

    > FS.RM myfs /hello.txt
    (integer) 1
    > EXISTS myfs
    (integer) 0

Read-only commands (`FS.CAT`, `FS.LS`, `FS.INFO`, etc.) against a
nonexistent key return an error rather than creating an empty filesystem.

## Reference of available commands

**FS.INFO: get filesystem statistics**

    FS.INFO key

Returns counts and totals about the filesystem as an array of
field-value pairs.

    > FS.INFO myfs
     1) "files"
     2) (integer) 47
     3) "directories"
     4) (integer) 12
     5) "symlinks"
     6) (integer) 3
     7) "total_data_bytes"
     8) (integer) 184320
     9) "total_inodes"
    10) (integer) 62

Time complexity: O(1) — all counters are maintained incrementally.

**FS.ECHO: write a file**

    FS.ECHO key path content

Creates or overwrites a file at the given path. The content is stored
as a binary-safe string — you can write text, JSON, binary blobs,
whatever.

If parent directories don't exist, they're created automatically
(like `mkdir -p` before the write). If the path already exists and
is a file, its content is replaced. If it exists but is a directory
or symlink, an error is returned.

    > FS.ECHO myfs /config.json '{"port": 8080}'
    OK

    > FS.ECHO myfs /deep/nested/path/file.txt "auto-created parents"
    OK

    > FS.LS myfs /deep/nested/path
    1) "file.txt"

Time complexity: O(d) where d is the path depth (for parent creation).

**FS.CAT: read a file**

    FS.CAT key path

Returns the content of a file. Follows symbolic links automatically
(up to 40 levels deep). Updates the file's access time.

Returns null if the path doesn't exist. Returns an error if the path
is a directory.

    > FS.CAT myfs /config.json
    "{\"port\": 8080}"

    > FS.CAT myfs /nonexistent
    (nil)

    > FS.CAT myfs /somedir
    (error) ERR not a file

Time complexity: O(1) for regular files, O(s) for symlinks where s is the chain length.

**FS.APPEND: append to a file**

    FS.APPEND key path content

Appends content to an existing file, or creates a new file if the
path doesn't exist. Returns the new total size in bytes.

Parent directories are created automatically, same as `FS.ECHO`.

    > FS.ECHO myfs /log.txt "line 1\n"
    OK
    > FS.APPEND myfs /log.txt "line 2\n"
    (integer) 14
    > FS.CAT myfs /log.txt
    "line 1\nline 2\n"

Time complexity: O(1) amortized for the append, O(d) if parents need creation.

**FS.RM: delete a file or directory**

    FS.RM key path [RECURSIVE]

Deletes the inode at the given path. For files and symlinks, this is
straightforward. For directories, the directory must be empty unless
`RECURSIVE` is specified, in which case the entire subtree is deleted
depth-first.

Returns 1 if something was deleted, 0 if the path didn't exist.
You cannot delete the root directory.

    > FS.RM myfs /old-file.txt
    (integer) 1

    > FS.RM myfs /nonempty-dir
    (error) ERR directory not empty — use RECURSIVE

    > FS.RM myfs /nonempty-dir RECURSIVE
    (integer) 1

    > FS.RM myfs /already-gone
    (integer) 0

    > FS.RM myfs /
    (error) ERR cannot delete root directory

Time complexity: O(1) for files, O(n) with RECURSIVE where n is the subtree size.

**FS.TOUCH: create or update timestamps**

    FS.TOUCH key path

If the path doesn't exist, creates an empty file (0 bytes). If it
does exist (file, directory, or symlink), updates its mtime and atime
to now.

Parent directories are created automatically.

    > FS.TOUCH myfs /marker.txt
    OK
    > FS.CAT myfs /marker.txt
    ""

Time complexity: O(d) where d is the path depth.

**FS.MKDIR: create a directory**

    FS.MKDIR key path [PARENTS]

Creates a directory. Without `PARENTS`, the parent directory must
already exist. With `PARENTS`, intermediate directories are created
as needed — equivalent to `mkdir -p`.

If the path already exists as a directory and `PARENTS` is specified,
this is a no-op (idempotent, same as POSIX `mkdir -p` behavior).
If it exists as anything else, an error is returned.

    > FS.MKDIR myfs /docs
    OK

    > FS.MKDIR myfs /a/b/c PARENTS
    OK

    > FS.MKDIR myfs /a/b/c PARENTS
    OK

    > FS.MKDIR myfs /existing-file.txt
    (error) ERR path already exists

Time complexity: O(d) where d is the path depth.

**FS.LS: list directory contents**

    FS.LS key path [LONG]

Returns the names of entries in a directory. Follows symlinks on the
directory path itself (so if `/link` points to `/realdir`, `FS.LS key /link`
lists the contents of `/realdir`).

Without `LONG`, returns a simple array of names. With `LONG`, each
entry is a 5-element array: `[name, type, mode, size, mtime]`.

    > FS.LS myfs /
    1) "config.json"
    2) "docs"
    3) "log.txt"

    > FS.LS myfs / LONG
    1) 1) "config.json"
       2) "file"
       3) "0644"
       4) (integer) 14
       5) (integer) 1709234567890
    2) 1) "docs"
       2) "dir"
       3) "0755"
       4) (integer) 0
       5) (integer) 1709234560000

Time complexity: O(n) where n is the number of entries. With `LONG`, O(n) dict lookups for child metadata.

**FS.STAT: get inode metadata**

    FS.STAT key path

Returns full metadata for a path as an array of field-value pairs.
Does *not* follow symlinks — if you stat a symlink, you get the
symlink's metadata, not the target's.

Returns null if the path doesn't exist.

For files, `size` is the content length in bytes. For directories,
`size` is the number of children.

    > FS.STAT myfs /config.json
     1) "type"
     2) "file"
     3) "mode"
     4) "0644"
     5) "uid"
     6) (integer) 0
     7) "gid"
     8) (integer) 0
     9) "size"
    10) (integer) 14
    11) "ctime"
    12) (integer) 1709234567890
    13) "mtime"
    14) (integer) 1709234567890
    15) "atime"
    16) (integer) 1709234567890

Time complexity: O(1).

**FS.TEST: check if a path exists**

    FS.TEST key path

Returns 1 if the path exists, 0 otherwise. Does not follow symlinks.
Named after `test -e` in shell.

    > FS.TEST myfs /config.json
    (integer) 1
    > FS.TEST myfs /nope
    (integer) 0

Time complexity: O(1).

**FS.CHMOD: change permission bits**

    FS.CHMOD key path mode

Sets the POSIX permission bits for a path. The mode is parsed as an
octal string (like `"0755"` or `"0644"`).

    > FS.CHMOD myfs /script.sh 0755
    OK
    > FS.STAT myfs /script.sh
    ...
     3) "mode"
     4) "0755"
    ...

Time complexity: O(1).

**FS.CHOWN: change ownership**

    FS.CHOWN key path uid [gid]

Sets the uid (and optionally gid) for a path. Both are stored as
unsigned 32-bit integers.

    > FS.CHOWN myfs /data 1000 1000
    OK

    > FS.CHOWN myfs /data 0
    OK

Time complexity: O(1).

**FS.LN: create a symbolic link**

    FS.LN key target linkpath

Creates a symbolic link at `linkpath` pointing to `target`. The target
is stored as-is — it can be an absolute path (`/etc/config`) or a
relative path (`../config`). Target resolution happens at read time
when another command follows the link.

Parent directories for `linkpath` are created automatically.

    > FS.LN myfs /config.json /shortcut
    OK
    > FS.CAT myfs /shortcut
    "{\"port\": 8080}"
    > FS.READLINK myfs /shortcut
    "/config.json"

Symlink chains are followed up to 40 levels deep. If you create a
cycle, commands that follow symlinks will return an error rather than
hang:

    > FS.LN myfs /b /a
    OK
    > FS.LN myfs /a /b
    OK
    > FS.CAT myfs /a
    (error) ERR too many levels of symbolic links

Time complexity: O(d) where d is the path depth.

**FS.READLINK: read a symlink target**

    FS.READLINK key path

Returns the raw target string of a symbolic link. Does not follow
the link — returns what was passed as the target argument to `FS.LN`.

Returns null if the path doesn't exist. Returns an error if the
path is not a symlink.

    > FS.READLINK myfs /shortcut
    "/config.json"

Time complexity: O(1).

**FS.CP: copy a file or directory**

    FS.CP key src dst [RECURSIVE]

Copies a file (or directory with `RECURSIVE`) from src to dst.
The destination must not already exist. Parent directories for the
destination are created automatically.

Copies preserve mode, uid, and gid from the source. Timestamps on
the copies are set to now.

    > FS.CP myfs /config.json /config.json.bak
    OK

    > FS.CP myfs /docs /docs-backup RECURSIVE
    OK

    > FS.CP myfs /docs /docs-backup
    (error) ERR source is a directory — use RECURSIVE

Time complexity: O(1) for files, O(n) for recursive where n is the subtree size.

**FS.MV: move or rename**

    FS.MV key src dst

Moves (renames) a file, directory, or symlink. For directories, all
descendants are moved atomically — the entire subtree is relocated
in the dict.

The destination must not exist. Parent directories for the
destination are created automatically. You cannot move the root.

    > FS.MV myfs /old-name.txt /new-name.txt
    OK

    > FS.MV myfs /src/components /lib/components
    OK

Time complexity: O(n) where n is the subtree size (all descendant paths must be rewritten in the dict).

**FS.TREE: recursive directory listing**

    FS.TREE key path [DEPTH depth]

Returns a tree view of the filesystem rooted at the given path.
The response is a nested array structure: directories are
`[name, [children...]]` and leaf nodes (files, symlinks, max-depth
dirs) are plain strings.

Files have no suffix, directories get a `/` suffix, symlinks get `@`.

    > FS.TREE myfs /
    1) "/"
    2) 1) "config.json"
       2) 1) "etc/"
          2) 1) 1) "nginx/"
                2) 1) "nginx.conf"
       3) "log.txt"

Use `DEPTH` to limit recursion. `DEPTH 1` shows only immediate children:

    > FS.TREE myfs / DEPTH 1
    1) "/"
    2) 1) "config.json"
       2) "etc/"
       3) "log.txt"

Time complexity: O(n) where n is the number of inodes in the subtree (bounded by DEPTH).

**FS.FIND: search for files by name**

    FS.FIND key path pattern [TYPE file|dir|symlink]

Walks the directory tree from `path` and returns all paths whose
basename matches the glob pattern. Supports `*` (any characters)
and `?` (single character) wildcards.

Use `TYPE` to filter results to a specific inode type.

    > FS.FIND myfs / "*.json"
    1) "/config.json"

    > FS.FIND myfs / "*.conf" TYPE file
    1) "/etc/nginx/nginx.conf"

    > FS.FIND myfs / "*" TYPE dir
    1) "/"
    2) "/etc"
    3) "/etc/nginx"

Time complexity: O(n) where n is the total number of inodes under the search path.

**FS.GREP: search file contents**

    FS.GREP key path pattern [NOCASE]

Searches the contents of all files under `path` for lines matching
the glob pattern. Returns an array of `[filepath, line_number, line]`
triples for each match.

This is a line-by-line glob match, not regex. Use `*` to match
any sequence: `*error*` matches any line containing "error".

Use `NOCASE` for case-insensitive matching.

    > FS.ECHO myfs /app.log "INFO: started\nERROR: disk full\nINFO: retrying"
    OK
    > FS.GREP myfs / "*ERROR*"
    1) 1) "/app.log"
       2) (integer) 2
       3) "ERROR: disk full"

    > FS.GREP myfs / "*error*" NOCASE
    1) 1) "/app.log"
       2) (integer) 2
       3) "ERROR: disk full"

Time complexity: O(n * m) where n is the number of files under the path and m is the average file size. This is a brute-force scan — it reads every byte of every file. For small to medium filesystems this is fine. For large ones with many files, consider keeping your search scope narrow by specifying a deeper path.

## Persistence

The filesystem is fully persisted via RDB. Every inode — its type,
metadata, content, children list, symlink target — is serialized
and restored on load. The RDB format is versioned (currently v0) so
future changes can be made without breaking existing dumps.

AOF rewrite is not currently implemented. The filesystem is a single
key, so standard Redis AOF command logging will replay the FS.*
commands that built it. This means AOF works correctly for durability,
it just doesn't have an optimized rewrite path yet.

`BGSAVE` works. `BGREWRITEAOF` works. Replication works (commands
are replicated verbatim).

## Memory usage

The `MEMORY USAGE` command reports an approximation for FS keys:

    > MEMORY USAGE myfs
    (integer) 4096

The estimate includes inode structs, dict overhead, and total file
content size. It's a lower bound — actual usage will be somewhat
higher due to allocator overhead and dict bucket arrays.

For rough planning: each inode costs about 120-200 bytes of overhead
(struct + dict entry + path string), plus the file content for files,
plus ~8 bytes per child name pointer for directories. A filesystem
with 10,000 small files will use roughly 2-3 MB of overhead plus
whatever the file contents total.

## Atomicity and concurrency

Every FS.* command is atomic — it runs in the Redis main thread as a
single operation. There's no locking, no transactions needed, no
partial state visible to other clients.

This means:

- `FS.ECHO` either fully replaces the file or doesn't
- `FS.MV` relocates an entire subtree atomically
- `FS.RM ... RECURSIVE` removes everything or nothing
- `FS.CP ... RECURSIVE` creates a complete copy in one shot

The tradeoff is that large operations block. A recursive delete of a
million-file subtree will block Redis for the duration. For most
use cases this is fine — filesystems with millions of inodes in a
single key are unusual. If you need that scale, partition across
multiple keys.

## Volumes and multi-tenancy

The old redis-fs-cli used a key naming convention (`fs:{volume}:meta:`,
`fs:{volume}:data:`, etc.) to implement volumes. The module approach
is simpler: **a volume is just a key**. The first write to a key
creates the filesystem automatically.

    > FS.ECHO project-alpha /README.md "Alpha project"
    OK
    > FS.ECHO project-beta /README.md "Beta project"
    OK
    > FS.ECHO staging /app.conf "port=8080"
    OK

To list all filesystems:

    > KEYS *
    1) "project-alpha"
    2) "project-beta"
    3) "staging"

Or better, use `SCAN` with a `TYPE` filter if you have other data types
in the same database:

    > SCAN 0 TYPE redis-fs0

To delete an entire filesystem, just delete the key:

    > DEL project-alpha
    (integer) 1

This means standard Redis features — expiration, renaming, access
control — all work on filesystems. `EXPIRE myfs 3600` gives you a
filesystem that auto-deletes in an hour. `RENAME staging production`
does what you'd expect.

## Limits and constraints

- **Path depth**: Normalized paths can have up to 256 components
- **Symlink depth**: Resolution follows up to 40 levels before erroring
- **Tree depth**: `FS.TREE` defaults to 64 levels max
- **File size**: No artificial limit — bounded by Redis memory. A single file can be as large as your available RAM allows. That said, very large files (hundreds of MB) will cause proportionally large allocations
- **Path format**: Always normalized to absolute. The module doesn't support or store relative paths internally
- **Character set**: Paths are binary-safe bytes, but `/` is always the separator and `\0` terminates. Stick to UTF-8 for sanity

## Performance characteristics

Most operations are dict lookups — O(1) in the average case.
Directory listings are O(n) in the child count. Recursive operations
(TREE, FIND, GREP, recursive CP/RM) are O(n) in the subtree size.

The critical insight is that path lookup is a hash table lookup, not
a directory-by-directory traversal. `FS.CAT myfs /a/b/c/d/e/f.txt`
doesn't walk six directories — it normalizes the path and does a
single dict lookup. This makes deep hierarchies essentially free
for point queries.

Write operations (ECHO, MKDIR, etc.) do update parent directories
to maintain the children array, which adds O(d) work where d is the
depth. But for a typical depth of 3-5, this is negligible.

## What this module does NOT do

- **FUSE mount**: This is a Redis data type, not a mountable filesystem. If you want to mount it, write a FUSE adapter that talks to Redis.
- **Access control**: Mode bits and uid/gid are stored but not enforced. They're metadata for your application to check, not a security boundary. Use Redis ACLs for access control.
- **File locking**: No `flock`, no advisory locks. Coordinate in your application or use Redis WATCH/MULTI if you need CAS semantics.
- **Extended attributes**: Phase 2. Coming as `FS.XATTR.SET/GET/DEL/LIST`.
- **Full-text search**: Use RediSearch with a custom indexer. The module stores the data, but doesn't maintain search indexes.
- **Vector embeddings**: Same — use Vector Sets alongside this module if you need semantic search over file contents.
- **Streaming / range reads**: `FS.CAT` returns the whole file. There's no `FS.CAT key /file OFFSET 1024 COUNT 4096` yet. If you need that, it's a reasonable Phase 2 addition.
