# Redis-FS

Redis-FS is a Redis module + FUSE mount stack that lets you use a Redis
key as a real filesystem.

This repository has three parts:

- `module/`: Redis module (`fs.so`) implementing `FS.*` commands and
  filesystem storage in one key.
- `mount/redis-fs-mount`: FUSE daemon that translates Linux file ops to
  `FS.*` commands.
- `rfs`: CLI orchestrator (recommended entrypoint) for bringing
  up Redis + mount daemons, status, teardown, and in-place migration.

## Recommended Workflow (CLI-first)

1. Build everything:

       make

2. Run the interactive setup

        ./rfs setup

3. Start managed services: 

        ./rfs up

3. Check status:

        ./rfs status

4. Stop managed services:

       ./rfs down

For migrating an existing local directory in place:

        ./rfs migrate <directory>

`migrate` imports files into Redis, renames the original directory to
`<dir>.archive`, and mounts Redis back at the original path.

## Project Layout

- `module/Makefile`: builds `module/fs.so`.
- `mount/Makefile`: builds `mount/redis-fs-mount`.
- `cli/Makefile`: builds `rfs` (output to repo root).
- root `Makefile`: orchestrator for all three.

Useful root targets:

- `make`: build module + mount binaries.
- `make module`: build only `module/fs.so`.
- `make mount`: build only mount binaries.
- `make clean`: clean all generated artifacts.

## Manual/Direct Setup (without `rfs`)

If you want full manual control:

    # Build
    make

    # Start Redis with module loaded
    ~/git/redis/src/redis-server --port 6379 --loadmodule ./module/fs.so

    # Seed a key
    redis-cli -p 6379 FS.ECHO myfs /hello.txt "Hello, World!"

    # Mount
    mkdir -p /tmp/mnt
    ./mount/redis-fs-mount --foreground myfs /tmp/mnt

## Module Command Mapping (Direct `FS.*` Usage)

If you've used a terminal, you already know how to use this module.
The commands are the same — just prefixed with `FS.` and taking a
key as the first argument. We dropped concepts that don't make sense
server-side (like `cd` and `pwd`, which are stateful client concerns)
and merged `rm` and `rmdir` into one command.

| Unix command                   | Redis command                      | Notes                                      |
|--------------------------------|------------------------------------|--------------------------------------------|
| cat file                       | FS.CAT key /file                   | Follows symlinks                           |
| echo "text" > file             | FS.ECHO key /file "text"           | Creates parents automatically              |
| echo "text" >> file            | FS.ECHO key /file "text" APPEND    | Creates file if missing; also FS.APPEND    |
| touch file                     | FS.TOUCH key /file                 | Creates or updates mtime                   |
| rm file                        | FS.RM key /file                    | Works on files, dirs, symlinks             |
| rm -r dir                      | FS.RM key /dir RECURSIVE           | Deletes entire subtree                     |
| rmdir dir                      | FS.RM key /dir                     | Fails if not empty (use RECURSIVE)         |
| mkdir dir                      | FS.MKDIR key /dir                  | Parent must exist                          |
| mkdir -p a/b/c                 | FS.MKDIR key /a/b/c PARENTS        | Creates intermediates                      |
| ls                             | FS.LS key                          | Lists root directory                       |
| ls dir                         | FS.LS key /dir                     | Returns child names                        |
| ls -l dir                      | FS.LS key /dir LONG                | Includes type, mode, size, mtime           |
| stat file                      | FS.STAT key /file                  | Full metadata: type, mode, uid, gid, times |
| test -e file                   | FS.TEST key /file                  | Returns 1 or 0                             |
| chmod 0755 file                | FS.CHMOD key /file 0755            | Octal mode string                          |
| chown uid:gid file             | FS.CHOWN key /file uid gid         | Separate uid and gid args                  |
| ln -s target link              | FS.LN key /target /link            | Target can be relative or absolute         |
| readlink link                  | FS.READLINK key /link              | Returns raw target string                  |
| cp src dst                     | FS.CP key /src /dst                | Files only without RECURSIVE               |
| cp -r src dst                  | FS.CP key /src /dst RECURSIVE      | Deep copy with metadata                    |
| mv src dst                     | FS.MV key /src /dst                | Moves entire subtrees atomically           |
| tree dir                       | FS.TREE key /dir                   | Nested array structure                     |
| tree -L 2 dir                  | FS.TREE key /dir DEPTH 2           | Limits recursion depth                     |
| find dir -name "*.txt"         | FS.FIND key /dir "*.txt"           | Full glob: *, ?, [a-z], [!x], \            |
| find dir -name "*.txt" -type f | FS.FIND key /dir "*.txt" TYPE file | Filter by type                             |
| grep -r "pattern" dir          | FS.GREP key /dir "*pattern*"       | Glob match on each line, bloom-accelerated |
| grep -ri "pattern" dir         | FS.GREP key /dir "*pattern*" NOCASE| Case-insensitive                           |
| truncate -s 100 file           | FS.TRUNCATE key /file 100          | Shrink, extend, or zero a file             |
| touch -t file                  | FS.UTIMENS key /file atime mtime   | Set times in ms; -1 = don't change         |
| df / du                        | FS.INFO key                        | File/dir/symlink counts + total bytes      |

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
field-value pairs. All counters are maintained incrementally, so this
command is always O(1).

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

**FS.ECHO: write a file**

    FS.ECHO key path content [APPEND]

Creates or overwrites a file at the given path. The content is stored
as a binary-safe string — you can write text, JSON, binary blobs,
whatever.

If parent directories don't exist, they're created automatically
(like `mkdir -p` before the write). If the path already exists and
is a file, its content is replaced. If it exists but is a directory
or symlink, an error is returned.

With `APPEND`, content is appended to an existing file instead of
overwriting it. If the file doesn't exist, it is created (same as
without `APPEND`). This is the equivalent of shell `echo >>`.

    > FS.ECHO myfs /config.json '{"port": 8080}'
    OK

    > FS.ECHO myfs /deep/nested/path/file.txt "auto-created parents"
    OK

    > FS.LS myfs /deep/nested/path
    1) "file.txt"

    > FS.ECHO myfs /log.txt "line 1\n"
    OK
    > FS.ECHO myfs /log.txt "line 2\n" APPEND
    OK
    > FS.CAT myfs /log.txt
    "line 1\nline 2\n"

The command is O(d) where d is the path depth, due to parent creation.

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

The command is O(1) for regular files, O(s) for symlinks where s is the chain length.

**FS.APPEND: append to a file**

    FS.APPEND key path content

Appends content to an existing file, or creates a new file if the
path doesn't exist. Returns the new total size in bytes.

Parent directories are created automatically, same as `FS.ECHO`.

Note: `FS.ECHO key path content APPEND` provides the same append
functionality. `FS.APPEND` is retained for backward compatibility.

    > FS.ECHO myfs /log.txt "line 1\n"
    OK
    > FS.APPEND myfs /log.txt "line 2\n"
    (integer) 14
    > FS.CAT myfs /log.txt
    "line 1\nline 2\n"

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

The command is O(1) for files, O(n) with RECURSIVE where n is the subtree size.

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

**FS.LS: list directory contents**

    FS.LS key [path] [LONG]

Returns the names of entries in a directory. If `path` is omitted,
lists the root directory `/`. Follows symlinks on the directory path
itself (so if `/link` points to `/realdir`, `FS.LS key /link` lists
the contents of `/realdir`).

Without `LONG`, returns a simple array of names. With `LONG`, each
entry is a 5-element array: `[name, type, mode, size, mtime]`.

    > FS.LS myfs
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

The command is O(n) where n is the number of entries. With `LONG`, each child requires a dict lookup for its metadata.

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

**FS.TEST: check if a path exists**

    FS.TEST key path

Returns 1 if the path exists, 0 otherwise. Does not follow symlinks.
Named after `test -e` in shell. The command is O(1).

    > FS.TEST myfs /config.json
    (integer) 1
    > FS.TEST myfs /nope
    (integer) 0

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

**FS.CHOWN: change ownership**

    FS.CHOWN key path uid [gid]

Sets the uid (and optionally gid) for a path. Both are stored as
unsigned 32-bit integers.

    > FS.CHOWN myfs /data 1000 1000
    OK

    > FS.CHOWN myfs /data 0
    OK

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

**FS.READLINK: read a symlink target**

    FS.READLINK key path

Returns the raw target string of a symbolic link. Does not follow
the link — returns what was passed as the target argument to `FS.LN`.

Returns null if the path doesn't exist. Returns an error if the
path is not a symlink.

    > FS.READLINK myfs /shortcut
    "/config.json"

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

The command is O(1) for files, O(n) for recursive where n is the subtree size.

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

The command is O(n) where n is the subtree size, since all descendant paths must be rewritten in the dict.

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

The command is O(n) where n is the number of inodes in the subtree (bounded by DEPTH).

**FS.FIND: search for files by name**

    FS.FIND key path pattern [TYPE file|dir|symlink]

Walks the directory tree from `path` and returns all paths whose
basename matches the glob pattern. Full glob syntax is supported:

- `*` — match zero or more characters
- `?` — match exactly one character
- `[abc]` — match one of a, b, or c
- `[a-z]` — match any character in range a through z (inclusive)
- `[!abc]` or `[^abc]` — match any character NOT in the set
- `\x` — match the literal character x (escape wildcards)

Use `TYPE` to filter results to a specific inode type.

    > FS.FIND myfs / "*.json"
    1) "/config.json"

    > FS.FIND myfs / "*.conf" TYPE file
    1) "/etc/nginx/nginx.conf"

    > FS.FIND myfs / "*" TYPE dir
    1) "/"
    2) "/etc"
    3) "/etc/nginx"

    > FS.FIND myfs / "[Rr]eadme*"
    1) "/README.md"

The command is O(n) where n is the total number of inodes under the search path.

**FS.GREP: search file contents**

    FS.GREP key path pattern [NOCASE]

Searches the contents of all files under `path` for lines matching
the glob pattern. Returns an array of `[filepath, line_number, line]`
triples for each match.

This is a line-by-line glob match, not regex. The same full glob
syntax as `FS.FIND` is supported: `*`, `?`, `[abc]`, `[a-z]`,
`[!x]`, and `\` escaping. Use `*` to match any sequence:
`*error*` matches any line containing "error".

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

    > FS.GREP myfs / "*[Ee][Rr][Rr][Oo][Rr]*"
    1) 1) "/app.log"
       2) (integer) 2
       3) "ERROR: disk full"

Files containing NUL bytes are treated as binary (same heuristic as
GNU grep). Instead of matching line by line, the pattern's literal
substring is checked against the raw content. Matches report line
number 0 and the text "Binary file matches":

    > FS.GREP myfs / "*pattern*"
    1) 1) "/data.bin"
       2) (integer) 0
       3) "Binary file matches"

Each file carries a 256-byte trigram bloom filter built from its
lowercased content. Before scanning a file, `FS.GREP` checks the
bloom filter against the pattern's longest literal substring. Files
that definitely don't contain the literal are skipped entirely,
which can significantly reduce scan time when searching large
filesystems with selective patterns.

The command is O(n * m) where n is the number of files under the path
and m is the average file size. The bloom filter prunes files that
definitely don't match, but worst-case every file must be scanned.
For large filesystems, keep your search scope narrow by specifying
a deeper path.

**FS.TRUNCATE: truncate or extend a file**

    FS.TRUNCATE key path length

Truncates or extends a file to the specified length in bytes. Follows
symlinks.

- If `length` is less than the current size, the file is shrunk
- If `length` is greater, the file is zero-extended
- If `length` is 0, the file content is cleared entirely

Updates the file's mtime.

    > FS.ECHO myfs /data.txt "Hello World"
    OK
    > FS.TRUNCATE myfs /data.txt 5
    OK
    > FS.CAT myfs /data.txt
    "Hello"
    > FS.TRUNCATE myfs /data.txt 0
    OK
    > FS.CAT myfs /data.txt
    ""

**FS.UTIMENS: set access and modification times**

    FS.UTIMENS key path atime_ms mtime_ms

Sets the access time and modification time for a path. Times are in
milliseconds since epoch. A value of `-1` means "don't change" (matches
POSIX `UTIME_OMIT` semantics).

Does NOT follow symlinks (matches POSIX `utimensat` with
`AT_SYMLINK_NOFOLLOW`).

    > FS.UTIMENS myfs /data.txt 1700000000000 1700000000000
    OK
    > FS.UTIMENS myfs /data.txt -1 1700000001000
    OK

# Glob pattern matching

Both `FS.FIND` and `FS.GREP` use the same glob matcher, modeled after
Redis's `stringmatchlen()` and POSIX `fnmatch()` semantics. The
supported syntax is:

| Pattern  | Meaning                       | Example                                       |
|----------|-------------------------------|-----------------------------------------------|
| `*`      | Zero or more characters       | `*.txt` matches `README.txt`                  |
| `?`      | Exactly one character         | `file?.log` matches `file1.log`               |
| `[abc]`  | One of the listed characters  | `[Mm]akefile` matches `Makefile` or `makefile` |
| `[a-z]`  | Character in range (inclusive) | `[0-9]*` matches names starting with a digit  |
| `[!abc]` | Any character NOT in the set  | `*.[!o]` matches any extension except `.o`    |
| `[^abc]` | Same as `[!abc]`              | Alternate negation syntax                     |
| `\x`     | Literal character x           | `file\*` matches the literal filename `file*` |

Character classes can contain ranges and individual characters mixed
together: `[a-zA-Z_]` matches any letter or underscore. Ranges work in
both directions: `[z-a]` is equivalent to `[a-z]`.

In `FS.FIND`, the pattern is matched against each entry's basename
(the filename, not the full path). In `FS.GREP`, the pattern is matched
against each line of file content.

# Persistence

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

# Memory usage

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

# Atomicity and concurrency

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

# Volumes and multi-tenancy

A volume is just a key. The first write to a key creates the
filesystem automatically.

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

# Performance characteristics

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

# Limits and constraints

- **Path depth**: Normalized paths can have up to 256 components
- **Symlink depth**: Resolution follows up to 40 levels before erroring
- **Tree depth**: `FS.TREE` defaults to 64 levels max
- **File size**: No artificial limit — bounded by Redis memory. A single file can be as large as your available RAM allows. That said, very large files (hundreds of MB) will cause proportionally large allocations
- **Path format**: Always normalized to absolute. The module doesn't support or store relative paths internally
- **Character set**: Paths are binary-safe bytes, but `/` is always the separator and `\0` terminates. Stick to UTF-8 for sanity

# FUSE Mount

The `mount/` directory contains `redis-fs-mount`, a Go FUSE daemon that
mounts a Redis FS key as a real Linux filesystem. Any program — `ls`,
`cat`, `vim`, `cp`, shell scripts, agents — can interact with the
Redis-backed filesystem using standard file operations.

## Building

Requires Go 1.21+ and FUSE support (`libfuse3-dev` or equivalent):

    cd mount
    go build -o redis-fs-mount ./cmd/redis-fs-mount

Or use the Makefile:

    cd mount
    make

## Usage

    redis-fs-mount [flags] <redis-key> <mountpoint>

**Example:**

    # Start Redis with the module
    redis-server --loadmodule ./module/fs.so

    # Seed some data
    redis-cli FS.ECHO myfs /hello.txt "Hello World"

    # Mount
    mkdir -p /tmp/mnt
    redis-fs-mount --foreground myfs /tmp/mnt

    # Use it like a normal filesystem
    cat /tmp/mnt/hello.txt          # → Hello World
    echo "new file" > /tmp/mnt/new.txt
    ls -la /tmp/mnt/
    mkdir /tmp/mnt/subdir
    mv /tmp/mnt/new.txt /tmp/mnt/subdir/

    # Verify via Redis
    redis-cli FS.CAT myfs /subdir/new.txt

    # Unmount
    fusermount -u /tmp/mnt

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--redis` | `localhost:6379` | Redis server address |
| `--password` | (none) | Redis password |
| `--db` | `0` | Redis database number |
| `--attr-timeout` | `1.0` | Attribute cache TTL in seconds |
| `--readonly` | `false` | Mount read-only |
| `--allow-other` | `false` | Allow other users to access mount |
| `--foreground` | `true` | Run in foreground |
| `--debug` | `false` | Enable FUSE debug logging (very verbose) |

## CLI Orchestrator

The `mount/` directory also provides `rfs`, an interactive
wizard that can start Redis + mount as daemons and manage their
lifecycle.

Build it:

    cd mount
    make rfs

Use it:

    # Interactive setup (prompts for Redis mode, mountpoint, key, etc.)
    ./rfs up

    # Show managed process + mount status
    ./rfs status

    # Migrate an existing local directory into Redis and mount in place
    ./rfs migrate

    # Unmount + stop managed daemons
    ./rfs down

`up` stores state in `~/.rfs/state.json` so later commands can
control the same processes across shell sessions.

`migrate` imports files into the selected Redis key, renames the source
directory to `<source>.archive` (or your chosen archive path), then
mounts the Redis-backed filesystem at the original source path.

## How it works

The daemon translates Linux VFS system calls into `FS.*` Redis commands:

| Operation | Redis command |
|-----------|---------------|
| `stat`, `ls` | `FS.STAT`, `FS.LS LONG` |
| `cat`, `read` | `FS.CAT` |
| `write`, `echo >` | `FS.ECHO` (buffered, flushed on close/fsync) |
| `touch`, `creat` | `FS.TOUCH` |
| `mkdir` | `FS.MKDIR PARENTS` |
| `rm`, `unlink` | `FS.RM` |
| `mv`, `rename` | `FS.MV` |
| `ln -s` | `FS.LN` |
| `readlink` | `FS.READLINK` |
| `chmod` | `FS.CHMOD` |
| `chown` | `FS.CHOWN` |
| `truncate` | `FS.TRUNCATE` |
| `utimes` | `FS.UTIMENS` |
| `df` | `FS.INFO` |

Writes are buffered in memory per file handle and flushed to Redis on
`close()` or `fsync()`. Attribute and directory listing results are
cached with a configurable TTL (default 1 second) to reduce Redis
round-trips.

All files appear owned by the mounting user's uid/gid, regardless of
what's stored in Redis (avoids permission issues for local use).

# Agent Integration

Redis-FS is designed for AI agents to store memories, documents, state, and tasks. We provide multiple integration options:

## Python Library

Install with pip:

    pip install -e ".[mcp]"

Use in your code:

```python
from redis_fs import RedisFS
import redis

r = redis.Redis()
fs = RedisFS(r, "agent-memory")

# Write and read
fs.write("/memories/context.md", "# Session Context\n...")
content = fs.read("/memories/context.md")

# Line-based editing (agent-friendly)
fs.replace("/tasks/todo.md", "- [ ] Task 1", "- [x] Task 1")
fs.insert("/notes/log.md", -1, "New entry\n")  # Append
fs.delete_lines("/draft.md", 10, 15)

# Search and navigate
files = fs.find("/", "*.md", type="file")
matches = fs.grep("/notes", "*TODO*", nocase=True)
```

## CLI

The `redis-fs` command provides Unix-like access:

    redis-fs cat myfs /file.txt
    redis-fs ls myfs /notes -l
    redis-fs grep myfs / "*TODO*" --nocase
    redis-fs replace myfs /file.md "old" "new" --all
    redis-fs echo myfs /log.txt "new line" --append

## MCP Server

For Claude, Auggie, and other MCP-compatible agents:

    # Start with Docker
    make mcp-up

    # Add to Auggie CLI
    make install-mcp-auggie

Or configure Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "redis-fs": {
      "command": "redis-fs-mcp",
      "args": ["--transport", "http", "--port", "8089"]
    }
  }
}
```

Available MCP tools: `fs_read`, `fs_write`, `fs_append`, `fs_lines`, `fs_replace`, `fs_insert`, `fs_delete_lines`, `fs_ls`, `fs_find`, `fs_grep`, `fs_mkdir`, `fs_rm`, `fs_info`

## Agent Skill

Install the skill for agent discovery:

    # Install to all agents (Claude Code, Cursor, Codex, etc.)
    make install-skill

    # Or just Claude Code
    make install-skill-local

The skill teaches agents:
- **When to use**: memories, markdown, state, tasks, logs
- **When NOT to use**: binaries, scripts, executables (can't run them)
- **All commands** with examples

## Docker Quick Start

    # Start Redis + MCP server
    make mcp-up

    # Test it
    curl http://localhost:8089/health
    redis-fs --port 6379 echo test /hello.txt "Hello!"

    # Stop
    make mcp-down

# What this module does NOT do

- **FUSE mount**: See the [FUSE Mount](#fuse-mount) section below for `redis-fs-mount`, a Go daemon that mounts a Redis FS key as a real Linux filesystem.
- **Access control**: Mode bits and uid/gid are stored but not enforced. They're metadata for your application to check, not a security boundary. Use Redis ACLs for access control.
- **File locking**: No `flock`, no advisory locks. Coordinate in your application or use Redis WATCH/MULTI if you need CAS semantics.
- **Extended attributes**: Phase 2. Coming as `FS.XATTR.SET/GET/DEL/LIST`.
- **Full-text search**: Use RediSearch with a custom indexer. The module stores the data, but doesn't maintain search indexes.
- **Vector embeddings**: Same — use Vector Sets alongside this module if you need semantic search over file contents.
- **Streaming / range reads**: `FS.CAT` returns the whole file. There's no `FS.CAT key /file OFFSET 1024 COUNT 4096` yet. If you need that, it's a reasonable Phase 2 addition.

## License

BSD-2-Clause
