/*
 * fs.c - Redis FS module.
 *
 * A native Redis module that implements a POSIX-like virtual filesystem
 * as a custom data type with an FS.* command family.
 *
 * Data model: one Redis key = one filesystem. Internally a flat dict
 * maps absolute paths to inodes. Each inode is a file, directory, or
 * symlink with POSIX metadata and inline content.
 *
 * Copyright (c) 2026, All rights reserved.
 * BSD-2-Clause license.
 */

#include "fs.h"
#include "path.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

/* Module type handle. */
RedisModuleType *FSType = NULL;

/* ===================================================================
 * Inode lifecycle
 * =================================================================== */

fsInode *fsInodeCreate(uint8_t type, uint16_t mode) {
    fsInode *inode = RedisModule_Alloc(sizeof(*inode));
    memset(inode, 0, sizeof(*inode));
    inode->type = type;

    if (mode == 0) {
        switch (type) {
        case FS_INODE_FILE:    mode = FS_DEFAULT_FILE_MODE; break;
        case FS_INODE_DIR:     mode = FS_DEFAULT_DIR_MODE; break;
        case FS_INODE_SYMLINK: mode = FS_DEFAULT_SYMLINK_MODE; break;
        }
    }
    inode->mode = mode;

    int64_t now = fsNowMs();
    inode->ctime = now;
    inode->mtime = now;
    inode->atime = now;

    if (type == FS_INODE_DIR) {
        inode->payload.dir.children = NULL;
        inode->payload.dir.count = 0;
        inode->payload.dir.capacity = 0;
    }
    return inode;
}

void fsInodeFree(fsInode *inode) {
    if (!inode) return;
    switch (inode->type) {
    case FS_INODE_FILE:
        if (inode->payload.file.data)
            RedisModule_Free(inode->payload.file.data);
        break;
    case FS_INODE_DIR:
        for (size_t i = 0; i < inode->payload.dir.count; i++)
            RedisModule_Free(inode->payload.dir.children[i]);
        if (inode->payload.dir.children)
            RedisModule_Free(inode->payload.dir.children);
        break;
    case FS_INODE_SYMLINK:
        if (inode->payload.symlink.target)
            RedisModule_Free(inode->payload.symlink.target);
        break;
    }
    RedisModule_Free(inode);
}

/* ===================================================================
 * Filesystem object lifecycle
 * =================================================================== */

fsObject *fsObjectCreate(void) {
    fsObject *fs = RedisModule_Alloc(sizeof(*fs));
    fs->inodes = RedisModule_CreateDict(NULL);
    fs->file_count = 0;
    fs->dir_count = 0;
    fs->symlink_count = 0;
    fs->total_data_size = 0;
    return fs;
}

void fsObjectFree(fsObject *fs) {
    if (!fs) return;

    /* Iterate and free all inodes. */
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(fs->inodes, "^", NULL, 0);
    char *key;
    size_t keylen;
    fsInode *inode;
    while ((key = RedisModule_DictNextC(iter, &keylen, (void**)&inode)) != NULL) {
        fsInodeFree(inode);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, fs->inodes);
    RedisModule_Free(fs);
}

/* ===================================================================
 * Directory helpers
 * =================================================================== */

void fsDirAddChild(fsInode *dir, const char *name, size_t namelen) {
    if (dir->type != FS_INODE_DIR) return;

    /* Check for duplicate. */
    for (size_t i = 0; i < dir->payload.dir.count; i++) {
        if (strlen(dir->payload.dir.children[i]) == namelen &&
            memcmp(dir->payload.dir.children[i], name, namelen) == 0)
            return; /* Already present. */
    }

    /* Grow array if needed. */
    if (dir->payload.dir.count >= dir->payload.dir.capacity) {
        size_t newcap = dir->payload.dir.capacity ? dir->payload.dir.capacity * 2 : 8;
        dir->payload.dir.children = RedisModule_Realloc(
            dir->payload.dir.children, sizeof(char*) * newcap);
        dir->payload.dir.capacity = newcap;
    }

    char *copy = RedisModule_Alloc(namelen + 1);
    memcpy(copy, name, namelen);
    copy[namelen] = '\0';
    dir->payload.dir.children[dir->payload.dir.count++] = copy;
}

int fsDirRemoveChild(fsInode *dir, const char *name, size_t namelen) {
    if (dir->type != FS_INODE_DIR) return 0;
    for (size_t i = 0; i < dir->payload.dir.count; i++) {
        if (strlen(dir->payload.dir.children[i]) == namelen &&
            memcmp(dir->payload.dir.children[i], name, namelen) == 0) {
            RedisModule_Free(dir->payload.dir.children[i]);
            /* Shift remaining elements. */
            for (size_t j = i; j + 1 < dir->payload.dir.count; j++)
                dir->payload.dir.children[j] = dir->payload.dir.children[j+1];
            dir->payload.dir.count--;
            return 1;
        }
    }
    return 0;
}

int fsDirHasChild(fsInode *dir, const char *name, size_t namelen) {
    if (dir->type != FS_INODE_DIR) return 0;
    for (size_t i = 0; i < dir->payload.dir.count; i++) {
        if (strlen(dir->payload.dir.children[i]) == namelen &&
            memcmp(dir->payload.dir.children[i], name, namelen) == 0)
            return 1;
    }
    return 0;
}

/* ===================================================================
 * File data helpers
 * =================================================================== */

void fsFileSetData(fsInode *inode, const char *data, size_t len) {
    if (inode->type != FS_INODE_FILE) return;
    if (inode->payload.file.data)
        RedisModule_Free(inode->payload.file.data);
    if (len > 0) {
        inode->payload.file.data = RedisModule_Alloc(len);
        memcpy(inode->payload.file.data, data, len);
    } else {
        inode->payload.file.data = NULL;
    }
    inode->payload.file.size = len;
}

void fsFileAppendData(fsInode *inode, const char *data, size_t len) {
    if (inode->type != FS_INODE_FILE || len == 0) return;
    size_t oldsize = inode->payload.file.size;
    size_t newsize = oldsize + len;
    inode->payload.file.data = RedisModule_Realloc(inode->payload.file.data, newsize);
    memcpy(inode->payload.file.data + oldsize, data, len);
    inode->payload.file.size = newsize;
}

/* ===================================================================
 * Lookup helpers
 * =================================================================== */

fsInode *fsLookup(fsObject *fs, const char *path, size_t pathlen) {
    int nokey = 0;
    void *val = RedisModule_DictGetC(fs->inodes, (void*)path, pathlen, &nokey);
    if (nokey) return NULL;
    return (fsInode*)val;
}

/* Insert an inode into the filesystem dict. Caller has allocated inode. */
static void fsInsert(fsObject *fs, const char *path, size_t pathlen, fsInode *inode) {
    RedisModule_DictSetC(fs->inodes, (void*)path, pathlen, inode);
    switch (inode->type) {
    case FS_INODE_FILE:    fs->file_count++; break;
    case FS_INODE_DIR:     fs->dir_count++; break;
    case FS_INODE_SYMLINK: fs->symlink_count++; break;
    }
}

/* Remove an inode from the filesystem dict. Does NOT free the inode. */
static fsInode *fsRemove(fsObject *fs, const char *path, size_t pathlen) {
    int nokey = 0;
    fsInode *inode = RedisModule_DictGetC(fs->inodes, (void*)path, pathlen, &nokey);
    if (nokey) return NULL;
    RedisModule_DictDelC(fs->inodes, (void*)path, pathlen, NULL);
    switch (inode->type) {
    case FS_INODE_FILE:
        fs->file_count--;
        fs->total_data_size -= inode->payload.file.size;
        break;
    case FS_INODE_DIR:     fs->dir_count--; break;
    case FS_INODE_SYMLINK: fs->symlink_count--; break;
    }
    return inode;
}

char *fsResolvePath(fsObject *fs, const char *path, size_t pathlen, int *err) {
    *err = 0;
    char *current = RedisModule_Alloc(pathlen + 1);
    memcpy(current, path, pathlen);
    current[pathlen] = '\0';

    for (int depth = 0; depth < FS_MAX_SYMLINK_DEPTH; depth++) {
        size_t clen = strlen(current);
        fsInode *inode = fsLookup(fs, current, clen);
        if (!inode) {
            /* Path not found — return as-is (caller decides). */
            return current;
        }
        if (inode->type != FS_INODE_SYMLINK) {
            return current;
        }
        /* Follow symlink. */
        char *target = inode->payload.symlink.target;
        size_t tlen = strlen(target);
        char *resolved;
        if (tlen > 0 && target[0] == '/') {
            resolved = fsNormalizePath(target, tlen);
        } else {
            char *parent = fsParentPath(current, clen);
            resolved = fsJoinPath(parent, strlen(parent), target, tlen);
            RedisModule_Free(parent);
        }
        RedisModule_Free(current);
        current = resolved;
    }

    /* Too many levels of symlinks. */
    RedisModule_Free(current);
    *err = 1;
    return NULL;
}

/* ===================================================================
 * Ensure parent directories exist for a path (mkdir -p style).
 * Returns 0 on success, -1 on error (e.g., a non-dir exists in the path).
 * =================================================================== */
static int fsEnsureParents(fsObject *fs, const char *path, size_t pathlen) {
    /* Walk from root to parent, creating dirs as needed. */
    char *parent = fsParentPath(path, pathlen);
    size_t plen = strlen(parent);

    if (fsIsRoot(parent, plen)) {
        /* Root should already exist. */
        RedisModule_Free(parent);
        return (fsLookup(fs, "/", 1) != NULL) ? 0 : -1;
    }

    /* Recursively ensure grandparent. */
    if (fsEnsureParents(fs, parent, plen) != 0) {
        RedisModule_Free(parent);
        return -1;
    }

    fsInode *existing = fsLookup(fs, parent, plen);
    if (existing) {
        if (existing->type != FS_INODE_DIR) {
            RedisModule_Free(parent);
            return -1; /* Not a directory. */
        }
        RedisModule_Free(parent);
        return 0;
    }

    /* Create the missing directory. */
    fsInode *dir = fsInodeCreate(FS_INODE_DIR, 0);
    fsInsert(fs, parent, plen, dir);

    /* Add to grandparent's children. */
    char *gp = fsParentPath(parent, plen);
    size_t gplen = strlen(gp);
    fsInode *gpnode = fsLookup(fs, gp, gplen);
    if (gpnode && gpnode->type == FS_INODE_DIR) {
        char *base = fsBaseName(parent, plen);
        fsDirAddChild(gpnode, base, strlen(base));
        RedisModule_Free(base);
    }
    RedisModule_Free(gp);
    RedisModule_Free(parent);
    return 0;
}

/* ===================================================================
 * Helper: open key and get fsObject, with error reply on failure.
 * mode: REDISMODULE_READ or REDISMODULE_READ|REDISMODULE_WRITE
 * Returns NULL on error (already sent reply). Sets *key_out.
 * =================================================================== */
static fsObject *fsGetObject(RedisModuleCtx *ctx, RedisModuleString *keyname,
                              int mode, RedisModuleKey **key_out) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, mode);
    int type = RedisModule_KeyType(key);

    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        if (mode & REDISMODULE_WRITE) {
            /* Auto-create: Redis convention — first write creates the key. */
            fsObject *fs = fsObjectCreate();
            fsInode *root = fsInodeCreate(FS_INODE_DIR, 0);
            fsInsert(fs, "/", 1, root);
            RedisModule_ModuleTypeSetValue(key, FSType, fs);
            *key_out = key;
            return fs;
        }
        RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");
        *key_out = NULL;
        return NULL;
    }

    if (RedisModule_ModuleTypeGetType(key) != FSType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        *key_out = NULL;
        return NULL;
    }

    *key_out = key;
    return RedisModule_ModuleTypeGetValue(key);
}

/* Helper: delete the key if the filesystem is empty (only root remains). */
static void fsMaybeDeleteKey(RedisModuleKey *key, fsObject *fs) {
    uint64_t total = fs->file_count + fs->dir_count + fs->symlink_count;
    if (total <= 1) {
        /* Only root "/" left (or somehow empty). Delete the key. */
        RedisModule_DeleteKey(key);
    }
}

/* ===================================================================
 * RDB persistence
 * =================================================================== */

/*
 * RDB format (version 0):
 *   uint64 inode_count
 *   For each inode:
 *     string  path
 *     uint8   type
 *     uint16  mode
 *     uint32  uid
 *     uint32  gid
 *     int64   ctime
 *     int64   mtime
 *     int64   atime
 *     [type-specific payload]
 *       FILE:    uint64 size + raw data
 *       DIR:     uint64 child_count + strings
 *       SYMLINK: string target
 */

void FSRdbSave(RedisModuleIO *rdb, void *value) {
    fsObject *fs = value;

    /* Count total inodes. */
    uint64_t count = fs->file_count + fs->dir_count + fs->symlink_count;
    RedisModule_SaveUnsigned(rdb, count);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(fs->inodes, "^", NULL, 0);
    char *path;
    size_t pathlen;
    fsInode *inode;
    while ((path = RedisModule_DictNextC(iter, &pathlen, (void**)&inode)) != NULL) {
        RedisModule_SaveStringBuffer(rdb, path, pathlen);
        RedisModule_SaveUnsigned(rdb, inode->type);
        RedisModule_SaveUnsigned(rdb, inode->mode);
        RedisModule_SaveUnsigned(rdb, inode->uid);
        RedisModule_SaveUnsigned(rdb, inode->gid);
        RedisModule_SaveSigned(rdb, inode->ctime);
        RedisModule_SaveSigned(rdb, inode->mtime);
        RedisModule_SaveSigned(rdb, inode->atime);

        switch (inode->type) {
        case FS_INODE_FILE:
            RedisModule_SaveUnsigned(rdb, inode->payload.file.size);
            if (inode->payload.file.size > 0)
                RedisModule_SaveStringBuffer(rdb, inode->payload.file.data,
                                              inode->payload.file.size);
            break;
        case FS_INODE_DIR:
            RedisModule_SaveUnsigned(rdb, inode->payload.dir.count);
            for (size_t i = 0; i < inode->payload.dir.count; i++) {
                size_t clen = strlen(inode->payload.dir.children[i]);
                RedisModule_SaveStringBuffer(rdb, inode->payload.dir.children[i], clen);
            }
            break;
        case FS_INODE_SYMLINK:
            RedisModule_SaveStringBuffer(rdb, inode->payload.symlink.target,
                                          strlen(inode->payload.symlink.target));
            break;
        }
    }
    RedisModule_DictIteratorStop(iter);
}

void *FSRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != 0) return NULL;

    fsObject *fs = fsObjectCreate();
    uint64_t count = RedisModule_LoadUnsigned(rdb);
    if (RedisModule_IsIOError(rdb)) goto ioerr;

    for (uint64_t i = 0; i < count; i++) {
        size_t pathlen;
        char *path = RedisModule_LoadStringBuffer(rdb, &pathlen);
        if (RedisModule_IsIOError(rdb)) goto ioerr;

        uint8_t type = RedisModule_LoadUnsigned(rdb);
        uint16_t mode = RedisModule_LoadUnsigned(rdb);
        uint32_t uid = RedisModule_LoadUnsigned(rdb);
        uint32_t gid = RedisModule_LoadUnsigned(rdb);
        int64_t ctime_val = RedisModule_LoadSigned(rdb);
        int64_t mtime = RedisModule_LoadSigned(rdb);
        int64_t atime = RedisModule_LoadSigned(rdb);
        if (RedisModule_IsIOError(rdb)) {
            RedisModule_Free(path);
            goto ioerr;
        }

        fsInode *inode = RedisModule_Alloc(sizeof(*inode));
        memset(inode, 0, sizeof(*inode));
        inode->type = type;
        inode->mode = mode;
        inode->uid = uid;
        inode->gid = gid;
        inode->ctime = ctime_val;
        inode->mtime = mtime;
        inode->atime = atime;

        switch (type) {
        case FS_INODE_FILE: {
            uint64_t size = RedisModule_LoadUnsigned(rdb);
            if (RedisModule_IsIOError(rdb)) {
                RedisModule_Free(path);
                RedisModule_Free(inode);
                goto ioerr;
            }
            if (size > 0) {
                size_t datalen;
                inode->payload.file.data = RedisModule_LoadStringBuffer(rdb, &datalen);
                inode->payload.file.size = datalen;
                if (RedisModule_IsIOError(rdb)) {
                    RedisModule_Free(path);
                    fsInodeFree(inode);
                    goto ioerr;
                }
            } else {
                inode->payload.file.data = NULL;
                inode->payload.file.size = 0;
            }
            fs->file_count++;
            fs->total_data_size += inode->payload.file.size;
            break;
        }
        case FS_INODE_DIR: {
            uint64_t nchildren = RedisModule_LoadUnsigned(rdb);
            if (RedisModule_IsIOError(rdb)) {
                RedisModule_Free(path);
                RedisModule_Free(inode);
                goto ioerr;
            }
            inode->payload.dir.count = 0;
            inode->payload.dir.capacity = nchildren ? nchildren : 0;
            inode->payload.dir.children = nchildren ?
                RedisModule_Alloc(sizeof(char*) * nchildren) : NULL;
            for (uint64_t j = 0; j < nchildren; j++) {
                size_t clen;
                char *child = RedisModule_LoadStringBuffer(rdb, &clen);
                if (RedisModule_IsIOError(rdb)) {
                    RedisModule_Free(path);
                    /* Free already loaded children. */
                    for (uint64_t k = 0; k < j; k++)
                        RedisModule_Free(inode->payload.dir.children[k]);
                    if (inode->payload.dir.children)
                        RedisModule_Free(inode->payload.dir.children);
                    RedisModule_Free(inode);
                    goto ioerr;
                }
                /* Ensure null-terminated. */
                char *copy = RedisModule_Alloc(clen + 1);
                memcpy(copy, child, clen);
                copy[clen] = '\0';
                RedisModule_Free(child);
                inode->payload.dir.children[inode->payload.dir.count++] = copy;
            }
            fs->dir_count++;
            break;
        }
        case FS_INODE_SYMLINK: {
            size_t tlen;
            char *target = RedisModule_LoadStringBuffer(rdb, &tlen);
            if (RedisModule_IsIOError(rdb)) {
                RedisModule_Free(path);
                RedisModule_Free(inode);
                goto ioerr;
            }
            inode->payload.symlink.target = RedisModule_Alloc(tlen + 1);
            memcpy(inode->payload.symlink.target, target, tlen);
            inode->payload.symlink.target[tlen] = '\0';
            RedisModule_Free(target);
            fs->symlink_count++;
            break;
        }
        default:
            RedisModule_Free(path);
            RedisModule_Free(inode);
            goto ioerr;
        }

        RedisModule_DictSetC(fs->inodes, path, pathlen, inode);
        RedisModule_Free(path);
    }

    return fs;

ioerr:
    fsObjectFree(fs);
    return NULL;
}

void FSFree(void *value) {
    fsObjectFree((fsObject*)value);
}

size_t FSMemUsage(const void *value) {
    const fsObject *fs = value;
    size_t mem = sizeof(fsObject);
    /* Approximate: dict overhead + inodes + data. */
    uint64_t total = fs->file_count + fs->dir_count + fs->symlink_count;
    mem += total * (sizeof(fsInode) + 64); /* inode + dict entry overhead */
    mem += fs->total_data_size;
    return mem;
}

void FSDigest(RedisModuleDigest *md, void *value) {
    fsObject *fs = value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(fs->inodes, "^", NULL, 0);
    char *path;
    size_t pathlen;
    fsInode *inode;
    while ((path = RedisModule_DictNextC(iter, &pathlen, (void**)&inode)) != NULL) {
        RedisModule_DigestAddStringBuffer(md, path, pathlen);
        RedisModule_DigestAddLongLong(md, inode->type);
        RedisModule_DigestAddLongLong(md, inode->mode);
        if (inode->type == FS_INODE_FILE && inode->payload.file.size > 0) {
            RedisModule_DigestAddStringBuffer(md, inode->payload.file.data,
                                               inode->payload.file.size);
        }
        RedisModule_DigestEndSequence(md);
    }
    RedisModule_DictIteratorStop(iter);
}

/* ===================================================================
 * FS.INFO key
 *
 * Returns filesystem statistics as a map.
 * =================================================================== */
int FSInfo_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) {
        return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");
    }

    RedisModule_ReplyWithArray(ctx, 10);
    RedisModule_ReplyWithCString(ctx, "files");
    RedisModule_ReplyWithLongLong(ctx, fs->file_count);
    RedisModule_ReplyWithCString(ctx, "directories");
    RedisModule_ReplyWithLongLong(ctx, fs->dir_count);
    RedisModule_ReplyWithCString(ctx, "symlinks");
    RedisModule_ReplyWithLongLong(ctx, fs->symlink_count);
    RedisModule_ReplyWithCString(ctx, "total_data_bytes");
    RedisModule_ReplyWithLongLong(ctx, fs->total_data_size);
    RedisModule_ReplyWithCString(ctx, "total_inodes");
    RedisModule_ReplyWithLongLong(ctx, fs->file_count + fs->dir_count + fs->symlink_count);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.ECHO key path content
 *
 * Write (create or overwrite) a file. Creates parent dirs automatically.
 * =================================================================== */
int FSEcho_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    /* fs is guaranteed non-NULL for write-mode opens (auto-created). */

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    if (fsIsRoot(path, npathlen)) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR cannot write to root directory");
    }

    /* Ensure parents exist. */
    if (fsEnsureParents(fs, path, npathlen) != 0) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR parent path conflict — a non-directory exists in the path");
    }

    size_t datalen;
    const char *data = RedisModule_StringPtrLen(argv[3], &datalen);

    fsInode *existing = fsLookup(fs, path, npathlen);
    if (existing) {
        if (existing->type != FS_INODE_FILE) {
            RedisModule_Free(path);
            return RedisModule_ReplyWithError(ctx, "ERR path exists and is not a file");
        }
        fs->total_data_size -= existing->payload.file.size;
        fsFileSetData(existing, data, datalen);
        fs->total_data_size += datalen;
        existing->mtime = fsNowMs();
    } else {
        fsInode *inode = fsInodeCreate(FS_INODE_FILE, 0);
        fsFileSetData(inode, data, datalen);
        fsInsert(fs, path, npathlen, inode);
        fs->total_data_size += datalen;

        /* Add to parent's children. */
        char *parent = fsParentPath(path, npathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (pnode && pnode->type == FS_INODE_DIR) {
            char *base = fsBaseName(path, npathlen);
            fsDirAddChild(pnode, base, strlen(base));
            RedisModule_Free(base);
            pnode->mtime = fsNowMs();
        }
        RedisModule_Free(parent);
    }

    RedisModule_Free(path);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.CAT key path
 *
 * Read file content. Follows symlinks.
 * =================================================================== */
int FSCat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    /* Resolve symlinks. */
    int err;
    char *resolved = fsResolvePath(fs, path, strlen(path), &err);
    RedisModule_Free(path);
    if (err) return RedisModule_ReplyWithError(ctx, "ERR too many levels of symbolic links");

    fsInode *inode = fsLookup(fs, resolved, strlen(resolved));
    RedisModule_Free(resolved);

    if (!inode)
        return RedisModule_ReplyWithNull(ctx);
    if (inode->type != FS_INODE_FILE)
        return RedisModule_ReplyWithError(ctx, "ERR not a file");

    inode->atime = fsNowMs();

    if (inode->payload.file.size == 0)
        return RedisModule_ReplyWithStringBuffer(ctx, "", 0);

    return RedisModule_ReplyWithStringBuffer(ctx, inode->payload.file.data,
                                              inode->payload.file.size);
}

/* ===================================================================
 * FS.APPEND key path content
 *
 * Append to a file. Creates the file if it doesn't exist.
 * Returns the new size.
 * =================================================================== */
int FSAppend_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    /* fs is guaranteed non-NULL for write-mode opens (auto-created). */

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    if (fsIsRoot(path, npathlen)) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR cannot append to root directory");
    }

    if (fsEnsureParents(fs, path, npathlen) != 0) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR parent path conflict");
    }

    size_t datalen;
    const char *data = RedisModule_StringPtrLen(argv[3], &datalen);

    fsInode *existing = fsLookup(fs, path, npathlen);
    if (existing) {
        if (existing->type != FS_INODE_FILE) {
            RedisModule_Free(path);
            return RedisModule_ReplyWithError(ctx, "ERR not a file");
        }
        fsFileAppendData(existing, data, datalen);
        fs->total_data_size += datalen;
        existing->mtime = fsNowMs();
        RedisModule_Free(path);
        RedisModule_ReplyWithLongLong(ctx, existing->payload.file.size);
    } else {
        fsInode *inode = fsInodeCreate(FS_INODE_FILE, 0);
        fsFileSetData(inode, data, datalen);
        fsInsert(fs, path, npathlen, inode);
        fs->total_data_size += datalen;

        char *parent = fsParentPath(path, npathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (pnode && pnode->type == FS_INODE_DIR) {
            char *base = fsBaseName(path, npathlen);
            fsDirAddChild(pnode, base, strlen(base));
            RedisModule_Free(base);
            pnode->mtime = fsNowMs();
        }
        RedisModule_Free(parent);
        RedisModule_Free(path);
        RedisModule_ReplyWithLongLong(ctx, datalen);
    }

    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.RM key path [RECURSIVE]
 *
 * Delete a file, directory, or symlink. Directories must be empty
 * unless RECURSIVE is specified.
 * =================================================================== */
static int fsDeleteRecursive(fsObject *fs, const char *path, size_t pathlen);

static int fsDeleteRecursive(fsObject *fs, const char *path, size_t pathlen) {
    fsInode *inode = fsLookup(fs, path, pathlen);
    if (!inode) return -1;

    if (inode->type == FS_INODE_DIR) {
        /* Delete children first. */
        /* We need to copy children list because we'll modify it. */
        size_t nchildren = inode->payload.dir.count;
        char **children_copy = NULL;
        if (nchildren > 0) {
            children_copy = RedisModule_Alloc(sizeof(char*) * nchildren);
            for (size_t i = 0; i < nchildren; i++) {
                size_t clen = strlen(inode->payload.dir.children[i]);
                children_copy[i] = RedisModule_Alloc(clen + 1);
                memcpy(children_copy[i], inode->payload.dir.children[i], clen + 1);
            }
        }

        for (size_t i = 0; i < nchildren; i++) {
            char *childpath = fsJoinPath(path, pathlen,
                                          children_copy[i], strlen(children_copy[i]));
            fsDeleteRecursive(fs, childpath, strlen(childpath));
            RedisModule_Free(childpath);
            RedisModule_Free(children_copy[i]);
        }
        if (children_copy) RedisModule_Free(children_copy);
    }

    /* Remove from parent's children. */
    if (!fsIsRoot(path, pathlen)) {
        char *parent = fsParentPath(path, pathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (pnode && pnode->type == FS_INODE_DIR) {
            char *base = fsBaseName(path, pathlen);
            fsDirRemoveChild(pnode, base, strlen(base));
            RedisModule_Free(base);
            pnode->mtime = fsNowMs();
        }
        RedisModule_Free(parent);
    }

    fsInode *removed = fsRemove(fs, path, pathlen);
    if (removed) fsInodeFree(removed);
    return 0;
}

int FSRm_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);

    int recursive = 0;
    if (argc == 4) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[3], &optlen);
        if (optlen == 9 && strncasecmp(opt, "RECURSIVE", 9) == 0) {
            recursive = 1;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected RECURSIVE");
        }
    }

    /* Open key — don't auto-create for delete (use read-check first). */
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
    int ktype = RedisModule_KeyType(key);
    if (ktype == REDISMODULE_KEYTYPE_EMPTY)
        return RedisModule_ReplyWithLongLong(ctx, 0);
    if (RedisModule_ModuleTypeGetType(key) != FSType)
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    fsObject *fs = RedisModule_ModuleTypeGetValue(key);

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    if (fsIsRoot(path, npathlen)) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR cannot delete root directory");
    }

    fsInode *inode = fsLookup(fs, path, npathlen);
    if (!inode) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithLongLong(ctx, 0);
    }

    if (inode->type == FS_INODE_DIR && inode->payload.dir.count > 0 && !recursive) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR directory not empty — use RECURSIVE");
    }

    if (recursive) {
        fsDeleteRecursive(fs, path, npathlen);
    } else {
        /* Remove from parent. */
        char *parent = fsParentPath(path, npathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (pnode && pnode->type == FS_INODE_DIR) {
            char *base = fsBaseName(path, npathlen);
            fsDirRemoveChild(pnode, base, strlen(base));
            RedisModule_Free(base);
            pnode->mtime = fsNowMs();
        }
        RedisModule_Free(parent);

        fsInode *removed = fsRemove(fs, path, npathlen);
        if (removed) fsInodeFree(removed);
    }

    RedisModule_Free(path);

    /* Redis convention: delete key when empty (only root left). */
    fsMaybeDeleteKey(key, fs);

    RedisModule_ReplyWithLongLong(ctx, 1);
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.TOUCH key path
 *
 * Create an empty file or update its mtime. Creates parent dirs.
 * =================================================================== */
int FSTouch_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    /* fs is guaranteed non-NULL for write-mode opens (auto-created). */

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    if (fsEnsureParents(fs, path, npathlen) != 0) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR parent path conflict");
    }

    fsInode *existing = fsLookup(fs, path, npathlen);
    if (existing) {
        existing->mtime = fsNowMs();
        existing->atime = fsNowMs();
    } else {
        fsInode *inode = fsInodeCreate(FS_INODE_FILE, 0);
        fsInsert(fs, path, npathlen, inode);

        char *parent = fsParentPath(path, npathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (pnode && pnode->type == FS_INODE_DIR) {
            char *base = fsBaseName(path, npathlen);
            fsDirAddChild(pnode, base, strlen(base));
            RedisModule_Free(base);
            pnode->mtime = fsNowMs();
        }
        RedisModule_Free(parent);
    }

    RedisModule_Free(path);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.MKDIR key path [PARENTS]
 *
 * Create a directory. With PARENTS, create intermediate dirs (mkdir -p).
 * =================================================================== */
int FSMkdir_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);

    int parents = 0;
    if (argc == 4) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[3], &optlen);
        if (optlen == 7 && strncasecmp(opt, "PARENTS", 7) == 0) {
            parents = 1;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected PARENTS");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    /* fs is guaranteed non-NULL for write-mode opens (auto-created). */

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    /* Check if already exists. */
    fsInode *existing = fsLookup(fs, path, npathlen);
    if (existing) {
        if (existing->type == FS_INODE_DIR && parents) {
            /* mkdir -p on existing dir is ok. */
            RedisModule_Free(path);
            return RedisModule_ReplyWithSimpleString(ctx, "OK");
        }
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR path already exists");
    }

    if (parents) {
        if (fsEnsureParents(fs, path, npathlen) != 0) {
            RedisModule_Free(path);
            return RedisModule_ReplyWithError(ctx, "ERR parent path conflict");
        }
    } else {
        /* Parent must exist. */
        char *parent = fsParentPath(path, npathlen);
        fsInode *pnode = fsLookup(fs, parent, strlen(parent));
        if (!pnode || pnode->type != FS_INODE_DIR) {
            RedisModule_Free(parent);
            RedisModule_Free(path);
            return RedisModule_ReplyWithError(ctx, "ERR parent directory does not exist");
        }
        RedisModule_Free(parent);
    }

    fsInode *dir = fsInodeCreate(FS_INODE_DIR, 0);
    fsInsert(fs, path, npathlen, dir);

    /* Add to parent's children. */
    char *parent = fsParentPath(path, npathlen);
    fsInode *pnode = fsLookup(fs, parent, strlen(parent));
    if (pnode && pnode->type == FS_INODE_DIR) {
        char *base = fsBaseName(path, npathlen);
        fsDirAddChild(pnode, base, strlen(base));
        RedisModule_Free(base);
        pnode->mtime = fsNowMs();
    }
    RedisModule_Free(parent);

    RedisModule_Free(path);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.LS key path [LONG]
 *
 * List directory contents. LONG returns metadata with each entry.
 * =================================================================== */
int FSLs_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3 || argc > 4) return RedisModule_WrongArity(ctx);

    int longformat = 0;
    if (argc == 4) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[3], &optlen);
        if (optlen == 4 && strncasecmp(opt, "LONG", 4) == 0) {
            longformat = 1;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected LONG");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    size_t npathlen = strlen(path);

    /* Resolve symlinks. */
    int err;
    char *resolved = fsResolvePath(fs, path, npathlen, &err);
    RedisModule_Free(path);
    if (err) return RedisModule_ReplyWithError(ctx, "ERR too many levels of symbolic links");

    fsInode *dir = fsLookup(fs, resolved, strlen(resolved));
    if (!dir) {
        RedisModule_Free(resolved);
        return RedisModule_ReplyWithError(ctx, "ERR no such directory");
    }
    if (dir->type != FS_INODE_DIR) {
        RedisModule_Free(resolved);
        return RedisModule_ReplyWithError(ctx, "ERR not a directory");
    }

    dir->atime = fsNowMs();

    if (!longformat) {
        RedisModule_ReplyWithArray(ctx, dir->payload.dir.count);
        for (size_t i = 0; i < dir->payload.dir.count; i++) {
            RedisModule_ReplyWithCString(ctx, dir->payload.dir.children[i]);
        }
    } else {
        /* Long format: each entry is [name, type, mode, size, mtime]. */
        RedisModule_ReplyWithArray(ctx, dir->payload.dir.count);
        for (size_t i = 0; i < dir->payload.dir.count; i++) {
            char *childpath = fsJoinPath(resolved, strlen(resolved),
                                          dir->payload.dir.children[i],
                                          strlen(dir->payload.dir.children[i]));
            fsInode *child = fsLookup(fs, childpath, strlen(childpath));
            RedisModule_Free(childpath);

            RedisModule_ReplyWithArray(ctx, 5);
            RedisModule_ReplyWithCString(ctx, dir->payload.dir.children[i]);
            if (child) {
                const char *typestr = "unknown";
                switch (child->type) {
                case FS_INODE_FILE: typestr = "file"; break;
                case FS_INODE_DIR: typestr = "dir"; break;
                case FS_INODE_SYMLINK: typestr = "symlink"; break;
                }
                RedisModule_ReplyWithCString(ctx, typestr);
                char modebuf[8];
                snprintf(modebuf, sizeof(modebuf), "%04o", child->mode);
                RedisModule_ReplyWithCString(ctx, modebuf);
                int64_t size = 0;
                if (child->type == FS_INODE_FILE) size = child->payload.file.size;
                RedisModule_ReplyWithLongLong(ctx, size);
                RedisModule_ReplyWithLongLong(ctx, child->mtime);
            } else {
                RedisModule_ReplyWithCString(ctx, "unknown");
                RedisModule_ReplyWithCString(ctx, "0000");
                RedisModule_ReplyWithLongLong(ctx, 0);
                RedisModule_ReplyWithLongLong(ctx, 0);
            }
        }
    }

    RedisModule_Free(resolved);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.STAT key path
 *
 * Returns metadata for a path as an array of field-value pairs.
 * =================================================================== */
int FSStat_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    fsInode *inode = fsLookup(fs, path, strlen(path));
    RedisModule_Free(path);

    if (!inode) return RedisModule_ReplyWithNull(ctx);

    /* Return 16 elements: 8 field-value pairs. */
    RedisModule_ReplyWithArray(ctx, 16);

    const char *typestr = "unknown";
    switch (inode->type) {
    case FS_INODE_FILE: typestr = "file"; break;
    case FS_INODE_DIR: typestr = "dir"; break;
    case FS_INODE_SYMLINK: typestr = "symlink"; break;
    }
    RedisModule_ReplyWithCString(ctx, "type");
    RedisModule_ReplyWithCString(ctx, typestr);

    char modebuf[8];
    snprintf(modebuf, sizeof(modebuf), "%04o", inode->mode);
    RedisModule_ReplyWithCString(ctx, "mode");
    RedisModule_ReplyWithCString(ctx, modebuf);

    RedisModule_ReplyWithCString(ctx, "uid");
    RedisModule_ReplyWithLongLong(ctx, inode->uid);

    RedisModule_ReplyWithCString(ctx, "gid");
    RedisModule_ReplyWithLongLong(ctx, inode->gid);

    int64_t size = 0;
    if (inode->type == FS_INODE_FILE)
        size = inode->payload.file.size;
    else if (inode->type == FS_INODE_DIR)
        size = inode->payload.dir.count;
    RedisModule_ReplyWithCString(ctx, "size");
    RedisModule_ReplyWithLongLong(ctx, size);

    RedisModule_ReplyWithCString(ctx, "ctime");
    RedisModule_ReplyWithLongLong(ctx, inode->ctime);

    RedisModule_ReplyWithCString(ctx, "mtime");
    RedisModule_ReplyWithLongLong(ctx, inode->mtime);

    RedisModule_ReplyWithCString(ctx, "atime");
    RedisModule_ReplyWithLongLong(ctx, inode->atime);

    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.TEST key path
 *
 * Returns 1 if the path exists, 0 otherwise.
 * =================================================================== */
int FSTest_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);
    fsInode *inode = fsLookup(fs, path, strlen(path));
    RedisModule_Free(path);

    return RedisModule_ReplyWithLongLong(ctx, inode ? 1 : 0);
}

/* ===================================================================
 * FS.CHMOD key path mode
 *
 * Change the mode (permission bits) of a path.
 * Mode is an octal string like "0755" or a decimal integer.
 * =================================================================== */
int FSChmod_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    fsInode *inode = fsLookup(fs, path, strlen(path));
    RedisModule_Free(path);
    if (!inode) return RedisModule_ReplyWithError(ctx, "ERR no such file or directory");

    size_t modelen;
    const char *modestr = RedisModule_StringPtrLen(argv[3], &modelen);
    unsigned int mode;
    /* Try octal parse first. */
    char modebuf[16];
    if (modelen >= sizeof(modebuf)) modelen = sizeof(modebuf) - 1;
    memcpy(modebuf, modestr, modelen);
    modebuf[modelen] = '\0';
    mode = (unsigned int)strtol(modebuf, NULL, 8);
    inode->mode = mode & 07777;

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.CHOWN key path uid [gid]
 *
 * Change the owner (and optionally group) of a path.
 * =================================================================== */
int FSChown_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    fsInode *inode = fsLookup(fs, path, strlen(path));
    RedisModule_Free(path);
    if (!inode) return RedisModule_ReplyWithError(ctx, "ERR no such file or directory");

    long long uid_val;
    if (RedisModule_StringToLongLong(argv[3], &uid_val) != REDISMODULE_OK) {
        return RedisModule_ReplyWithError(ctx, "ERR uid must be an integer");
    }
    inode->uid = (uint32_t)uid_val;

    if (argc == 5) {
        long long gid_val;
        if (RedisModule_StringToLongLong(argv[4], &gid_val) != REDISMODULE_OK) {
            return RedisModule_ReplyWithError(ctx, "ERR gid must be an integer");
        }
        inode->gid = (uint32_t)gid_val;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.LN key target linkpath
 *
 * Create a symbolic link at linkpath pointing to target.
 * =================================================================== */
int FSLn_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    /* fs is guaranteed non-NULL for write-mode opens (auto-created). */

    size_t targetlen;
    const char *target = RedisModule_StringPtrLen(argv[2], &targetlen);

    size_t linkpathlen;
    const char *rawlinkpath = RedisModule_StringPtrLen(argv[3], &linkpathlen);
    char *linkpath = fsNormalizePath(rawlinkpath, linkpathlen);
    size_t nlinklen = strlen(linkpath);

    if (fsIsRoot(linkpath, nlinklen)) {
        RedisModule_Free(linkpath);
        return RedisModule_ReplyWithError(ctx, "ERR cannot create symlink at root");
    }

    if (fsLookup(fs, linkpath, nlinklen)) {
        RedisModule_Free(linkpath);
        return RedisModule_ReplyWithError(ctx, "ERR path already exists");
    }

    if (fsEnsureParents(fs, linkpath, nlinklen) != 0) {
        RedisModule_Free(linkpath);
        return RedisModule_ReplyWithError(ctx, "ERR parent path conflict");
    }

    fsInode *inode = fsInodeCreate(FS_INODE_SYMLINK, 0);
    inode->payload.symlink.target = RedisModule_Alloc(targetlen + 1);
    memcpy(inode->payload.symlink.target, target, targetlen);
    inode->payload.symlink.target[targetlen] = '\0';
    fsInsert(fs, linkpath, nlinklen, inode);

    char *parent = fsParentPath(linkpath, nlinklen);
    fsInode *pnode = fsLookup(fs, parent, strlen(parent));
    if (pnode && pnode->type == FS_INODE_DIR) {
        char *base = fsBaseName(linkpath, nlinklen);
        fsDirAddChild(pnode, base, strlen(base));
        RedisModule_Free(base);
        pnode->mtime = fsNowMs();
    }
    RedisModule_Free(parent);

    RedisModule_Free(linkpath);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.READLINK key path
 *
 * Returns the target of a symbolic link.
 * =================================================================== */
int FSReadlink_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 3) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    fsInode *inode = fsLookup(fs, path, strlen(path));
    RedisModule_Free(path);

    if (!inode) return RedisModule_ReplyWithNull(ctx);
    if (inode->type != FS_INODE_SYMLINK)
        return RedisModule_ReplyWithError(ctx, "ERR not a symbolic link");

    return RedisModule_ReplyWithCString(ctx, inode->payload.symlink.target);
}

/* ===================================================================
 * FS.CP key src dst [RECURSIVE]
 *
 * Copy a file or directory.
 * =================================================================== */
static int fsCopyRecursive(fsObject *fs, const char *src, size_t srclen,
                            const char *dst, size_t dstlen);

static int fsCopyRecursive(fsObject *fs, const char *src, size_t srclen,
                            const char *dst, size_t dstlen) {
    fsInode *sinode = fsLookup(fs, src, srclen);
    if (!sinode) return -1;

    if (sinode->type == FS_INODE_FILE) {
        fsInode *newinode = fsInodeCreate(FS_INODE_FILE, sinode->mode);
        newinode->uid = sinode->uid;
        newinode->gid = sinode->gid;
        if (sinode->payload.file.size > 0) {
            fsFileSetData(newinode, sinode->payload.file.data, sinode->payload.file.size);
        }
        fsInsert(fs, dst, dstlen, newinode);
        fs->total_data_size += newinode->payload.file.size;
        return 0;
    } else if (sinode->type == FS_INODE_DIR) {
        fsInode *newdir = fsInodeCreate(FS_INODE_DIR, sinode->mode);
        newdir->uid = sinode->uid;
        newdir->gid = sinode->gid;
        fsInsert(fs, dst, dstlen, newdir);

        for (size_t i = 0; i < sinode->payload.dir.count; i++) {
            char *childname = sinode->payload.dir.children[i];
            size_t cnamelen = strlen(childname);
            char *srcc = fsJoinPath(src, srclen, childname, cnamelen);
            char *dstc = fsJoinPath(dst, dstlen, childname, cnamelen);
            fsDirAddChild(newdir, childname, cnamelen);
            fsCopyRecursive(fs, srcc, strlen(srcc), dstc, strlen(dstc));
            RedisModule_Free(srcc);
            RedisModule_Free(dstc);
        }
        return 0;
    } else if (sinode->type == FS_INODE_SYMLINK) {
        fsInode *newlink = fsInodeCreate(FS_INODE_SYMLINK, sinode->mode);
        char *target = sinode->payload.symlink.target;
        size_t tlen = strlen(target);
        newlink->payload.symlink.target = RedisModule_Alloc(tlen + 1);
        memcpy(newlink->payload.symlink.target, target, tlen + 1);
        fsInsert(fs, dst, dstlen, newlink);
        return 0;
    }
    return -1;
}

int FSCp_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

    int recursive = 0;
    if (argc == 5) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[4], &optlen);
        if (optlen == 9 && strncasecmp(opt, "RECURSIVE", 9) == 0) {
            recursive = 1;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected RECURSIVE");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t srclen;
    const char *rawsrc = RedisModule_StringPtrLen(argv[2], &srclen);
    char *src = fsNormalizePath(rawsrc, srclen);
    size_t nsrclen = strlen(src);

    size_t dstlen;
    const char *rawdst = RedisModule_StringPtrLen(argv[3], &dstlen);
    char *dst = fsNormalizePath(rawdst, dstlen);
    size_t ndstlen = strlen(dst);

    fsInode *sinode = fsLookup(fs, src, nsrclen);
    if (!sinode) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR source not found");
    }

    if (sinode->type == FS_INODE_DIR && !recursive) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR source is a directory — use RECURSIVE");
    }

    if (fsLookup(fs, dst, ndstlen)) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR destination already exists");
    }

    if (fsEnsureParents(fs, dst, ndstlen) != 0) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR destination parent path conflict");
    }

    fsCopyRecursive(fs, src, nsrclen, dst, ndstlen);

    /* Add to parent's children. */
    char *parent = fsParentPath(dst, ndstlen);
    fsInode *pnode = fsLookup(fs, parent, strlen(parent));
    if (pnode && pnode->type == FS_INODE_DIR) {
        char *base = fsBaseName(dst, ndstlen);
        fsDirAddChild(pnode, base, strlen(base));
        RedisModule_Free(base);
        pnode->mtime = fsNowMs();
    }
    RedisModule_Free(parent);

    RedisModule_Free(src);
    RedisModule_Free(dst);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.MV key src dst
 *
 * Move/rename a file or directory.
 * =================================================================== */
int FSMv_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 4) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t srclen;
    const char *rawsrc = RedisModule_StringPtrLen(argv[2], &srclen);
    char *src = fsNormalizePath(rawsrc, srclen);
    size_t nsrclen = strlen(src);

    size_t dstlen;
    const char *rawdst = RedisModule_StringPtrLen(argv[3], &dstlen);
    char *dst = fsNormalizePath(rawdst, dstlen);
    size_t ndstlen = strlen(dst);

    if (fsIsRoot(src, nsrclen)) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR cannot move root");
    }

    fsInode *sinode = fsLookup(fs, src, nsrclen);
    if (!sinode) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR source not found");
    }

    if (fsLookup(fs, dst, ndstlen)) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR destination already exists");
    }

    if (fsEnsureParents(fs, dst, ndstlen) != 0) {
        RedisModule_Free(src);
        RedisModule_Free(dst);
        return RedisModule_ReplyWithError(ctx, "ERR destination parent path conflict");
    }

    /* For directories, we need to move all descendants too. */
    if (sinode->type == FS_INODE_DIR) {
        /* Collect all paths under src. */
        /* We iterate the dict and find paths starting with src + "/". */
        size_t prefixlen = nsrclen;
        /* Build prefix for children: src + "/" */
        char *prefix = RedisModule_Alloc(prefixlen + 2);
        memcpy(prefix, src, nsrclen);
        prefix[nsrclen] = '/';
        prefix[nsrclen + 1] = '\0';
        prefixlen = nsrclen + 1;

        /* Collect paths to move. */
        typedef struct { char *oldpath; size_t oldlen; } movepath;
        movepath *moves = NULL;
        size_t nmoves = 0, movecap = 0;

        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(
            fs->inodes, ">=", prefix, prefixlen);
        char *k;
        size_t klen;
        void *v;
        while ((k = RedisModule_DictNextC(iter, &klen, &v)) != NULL) {
            if (klen < prefixlen || memcmp(k, prefix, prefixlen) != 0) break;
            if (nmoves >= movecap) {
                movecap = movecap ? movecap * 2 : 32;
                moves = RedisModule_Realloc(moves, sizeof(movepath) * movecap);
            }
            moves[nmoves].oldpath = RedisModule_Alloc(klen + 1);
            memcpy(moves[nmoves].oldpath, k, klen);
            moves[nmoves].oldpath[klen] = '\0';
            moves[nmoves].oldlen = klen;
            nmoves++;
        }
        RedisModule_DictIteratorStop(iter);

        /* Move descendants. */
        for (size_t i = 0; i < nmoves; i++) {
            /* New path = dst + suffix after src. */
            const char *suffix = moves[i].oldpath + nsrclen;
            size_t suffixlen = moves[i].oldlen - nsrclen;
            char *newpath = fsJoinPath(dst, ndstlen, suffix, suffixlen);
            size_t newlen = strlen(newpath);

            fsInode *inode_val = fsLookup(fs, moves[i].oldpath, moves[i].oldlen);
            if (inode_val) {
                RedisModule_DictDelC(fs->inodes, moves[i].oldpath, moves[i].oldlen, NULL);
                RedisModule_DictSetC(fs->inodes, newpath, newlen, inode_val);
            }
            RedisModule_Free(newpath);
            RedisModule_Free(moves[i].oldpath);
        }
        if (moves) RedisModule_Free(moves);
        RedisModule_Free(prefix);
    }

    /* Move the inode itself. */
    RedisModule_DictDelC(fs->inodes, src, nsrclen, NULL);
    RedisModule_DictSetC(fs->inodes, dst, ndstlen, sinode);

    /* Update old parent. */
    char *oldparent = fsParentPath(src, nsrclen);
    fsInode *opnode = fsLookup(fs, oldparent, strlen(oldparent));
    if (opnode && opnode->type == FS_INODE_DIR) {
        char *oldbase = fsBaseName(src, nsrclen);
        fsDirRemoveChild(opnode, oldbase, strlen(oldbase));
        RedisModule_Free(oldbase);
        opnode->mtime = fsNowMs();
    }
    RedisModule_Free(oldparent);

    /* Update new parent. */
    char *newparent = fsParentPath(dst, ndstlen);
    fsInode *npnode = fsLookup(fs, newparent, strlen(newparent));
    if (npnode && npnode->type == FS_INODE_DIR) {
        char *newbase = fsBaseName(dst, ndstlen);
        fsDirAddChild(npnode, newbase, strlen(newbase));
        RedisModule_Free(newbase);
        npnode->mtime = fsNowMs();
    }
    RedisModule_Free(newparent);

    RedisModule_Free(src);
    RedisModule_Free(dst);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.TREE key path [DEPTH depth]
 *
 * Returns a tree view of the filesystem rooted at path.
 * Response is a nested array structure.
 * =================================================================== */
static void fsTreeReply(RedisModuleCtx *ctx, fsObject *fs,
                         const char *path, size_t pathlen,
                         int depth, int maxdepth) {
    fsInode *inode = fsLookup(fs, path, pathlen);
    if (!inode) return;

    char *base = fsBaseName(path, pathlen);

    if (inode->type != FS_INODE_DIR || depth >= maxdepth) {
        /* Leaf node: just the name. */
        const char *suffix = "";
        if (inode->type == FS_INODE_DIR) suffix = "/";
        else if (inode->type == FS_INODE_SYMLINK) suffix = "@";

        size_t blen = strlen(base);
        size_t slen = strlen(suffix);
        char *display = RedisModule_Alloc(blen + slen + 1);
        memcpy(display, base, blen);
        memcpy(display + blen, suffix, slen);
        display[blen + slen] = '\0';

        RedisModule_ReplyWithCString(ctx, display);
        RedisModule_Free(display);
        RedisModule_Free(base);
        return;
    }

    /* Directory: [name, [child1, child2, ...]] */
    RedisModule_ReplyWithArray(ctx, 2);

    /* Root "/" should display as "/" not "//". */
    if (fsIsRoot(path, pathlen)) {
        RedisModule_ReplyWithCString(ctx, "/");
    } else {
        char *dirname = RedisModule_Alloc(strlen(base) + 2);
        sprintf(dirname, "%s/", base);
        RedisModule_ReplyWithCString(ctx, dirname);
        RedisModule_Free(dirname);
    }
    RedisModule_Free(base);

    RedisModule_ReplyWithArray(ctx, inode->payload.dir.count);
    for (size_t i = 0; i < inode->payload.dir.count; i++) {
        char *childpath = fsJoinPath(path, pathlen,
                                      inode->payload.dir.children[i],
                                      strlen(inode->payload.dir.children[i]));
        fsTreeReply(ctx, fs, childpath, strlen(childpath), depth + 1, maxdepth);
        RedisModule_Free(childpath);
    }
}

int FSTree_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3 || argc > 5) return RedisModule_WrongArity(ctx);

    int maxdepth = FS_MAX_TREE_DEPTH;
    if (argc >= 5) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[3], &optlen);
        if (optlen == 5 && strncasecmp(opt, "DEPTH", 5) == 0) {
            long long d;
            if (RedisModule_StringToLongLong(argv[4], &d) != REDISMODULE_OK || d < 0) {
                return RedisModule_ReplyWithError(ctx, "ERR DEPTH must be a non-negative integer");
            }
            maxdepth = (int)d;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected DEPTH <n>");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    fsInode *inode = fsLookup(fs, path, strlen(path));
    if (!inode) {
        RedisModule_Free(path);
        return RedisModule_ReplyWithError(ctx, "ERR no such path");
    }

    fsTreeReply(ctx, fs, path, strlen(path), 0, maxdepth);

    RedisModule_Free(path);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.FIND key path pattern [TYPE file|dir|symlink]
 *
 * Find files matching a glob pattern. DFS from the given path.
 * Returns an array of matching paths.
 * =================================================================== */
static void fsFindWalk(fsObject *fs, const char *path, size_t pathlen,
                        const char *pattern, int typefilter,
                        RedisModuleCtx *ctx, long *count) {
    fsInode *inode = fsLookup(fs, path, pathlen);
    if (!inode) return;

    /* Check if this path matches. */
    char *base = fsBaseName(path, pathlen);
    if (fsGlobMatch(pattern, base)) {
        if (typefilter < 0 || typefilter == inode->type) {
            RedisModule_ReplyWithCString(ctx, path);
            (*count)++;
        }
    }
    RedisModule_Free(base);

    /* Recurse into directories. */
    if (inode->type == FS_INODE_DIR) {
        for (size_t i = 0; i < inode->payload.dir.count; i++) {
            char *childpath = fsJoinPath(path, pathlen,
                                          inode->payload.dir.children[i],
                                          strlen(inode->payload.dir.children[i]));
            fsFindWalk(fs, childpath, strlen(childpath), pattern, typefilter, ctx, count);
            RedisModule_Free(childpath);
        }
    }
}

int FSFind_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4 || argc > 6) return RedisModule_WrongArity(ctx);

    int typefilter = -1; /* -1 = all types */
    if (argc >= 6) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[4], &optlen);
        if (optlen == 4 && strncasecmp(opt, "TYPE", 4) == 0) {
            size_t tlen;
            const char *tstr = RedisModule_StringPtrLen(argv[5], &tlen);
            if (tlen == 4 && strncasecmp(tstr, "file", 4) == 0) typefilter = FS_INODE_FILE;
            else if (tlen == 3 && strncasecmp(tstr, "dir", 3) == 0) typefilter = FS_INODE_DIR;
            else if (tlen == 7 && strncasecmp(tstr, "symlink", 7) == 0) typefilter = FS_INODE_SYMLINK;
            else return RedisModule_ReplyWithError(ctx, "ERR TYPE must be file, dir, or symlink");
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected TYPE <type>");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    size_t patternlen;
    const char *pattern = RedisModule_StringPtrLen(argv[3], &patternlen);

    /* Use postponed array length since we don't know how many matches. */
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    long count = 0;
    fsFindWalk(fs, path, strlen(path), pattern, typefilter, ctx, &count);
    RedisModule_ReplySetArrayLength(ctx, count);

    RedisModule_Free(path);
    return REDISMODULE_OK;
}

/* ===================================================================
 * FS.GREP key path pattern [NOCASE]
 *
 * Search file contents under path for lines matching pattern.
 * Returns array of [filepath, line_number, line_content] triples.
 * =================================================================== */
static void fsGrepWalk(fsObject *fs, const char *path, size_t pathlen,
                        const char *pattern, int nocase,
                        RedisModuleCtx *ctx, long *count) {
    fsInode *inode = fsLookup(fs, path, pathlen);
    if (!inode) return;

    if (inode->type == FS_INODE_FILE && inode->payload.file.size > 0) {
        /* Search line by line. */
        const char *data = inode->payload.file.data;
        size_t size = inode->payload.file.size;
        int lineno = 1;
        size_t pos = 0;

        while (pos < size) {
            /* Find line end. */
            size_t linestart = pos;
            while (pos < size && data[pos] != '\n') pos++;
            size_t linelen = pos - linestart;
            if (pos < size) pos++; /* skip newline */

            /* Extract line as null-terminated string. */
            char *line = RedisModule_Alloc(linelen + 1);
            memcpy(line, data + linestart, linelen);
            line[linelen] = '\0';

            int match;
            if (nocase)
                match = fsGlobMatchNoCase(pattern, line);
            else
                match = fsGlobMatch(pattern, line);

            if (match) {
                RedisModule_ReplyWithArray(ctx, 3);
                RedisModule_ReplyWithCString(ctx, path);
                RedisModule_ReplyWithLongLong(ctx, lineno);
                RedisModule_ReplyWithStringBuffer(ctx, line, linelen);
                (*count)++;
            }

            RedisModule_Free(line);
            lineno++;
        }
    }

    if (inode->type == FS_INODE_DIR) {
        for (size_t i = 0; i < inode->payload.dir.count; i++) {
            char *childpath = fsJoinPath(path, pathlen,
                                          inode->payload.dir.children[i],
                                          strlen(inode->payload.dir.children[i]));
            fsGrepWalk(fs, childpath, strlen(childpath), pattern, nocase, ctx, count);
            RedisModule_Free(childpath);
        }
    }
}

int FSGrep_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4 || argc > 5) return RedisModule_WrongArity(ctx);

    int nocase = 0;
    if (argc == 5) {
        size_t optlen;
        const char *opt = RedisModule_StringPtrLen(argv[4], &optlen);
        if (optlen == 6 && strncasecmp(opt, "NOCASE", 6) == 0) {
            nocase = 1;
        } else {
            return RedisModule_ReplyWithError(ctx, "ERR syntax error — expected NOCASE");
        }
    }

    RedisModuleKey *key;
    fsObject *fs = fsGetObject(ctx, argv[1], REDISMODULE_READ, &key);
    if (!key) return REDISMODULE_OK;
    if (!fs) return RedisModule_ReplyWithError(ctx, "ERR no such filesystem key");

    size_t pathlen;
    const char *rawpath = RedisModule_StringPtrLen(argv[2], &pathlen);
    char *path = fsNormalizePath(rawpath, pathlen);

    size_t patternlen;
    const char *pattern = RedisModule_StringPtrLen(argv[3], &patternlen);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_LEN);
    long count = 0;
    fsGrepWalk(fs, path, strlen(path), pattern, nocase, ctx, &count);
    RedisModule_ReplySetArrayLength(ctx, count);

    RedisModule_Free(path);
    return REDISMODULE_OK;
}

/* ===================================================================
 * Module OnLoad — register type and commands
 * =================================================================== */

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "fs", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModule_SetModuleOptions(ctx,
        REDISMODULE_OPTIONS_HANDLE_IO_ERRORS |
        REDISMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD);

    /* Register the custom data type. */
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = FSRdbLoad,
        .rdb_save = FSRdbSave,
        .aof_rewrite = NULL,
        .mem_usage = FSMemUsage,
        .free = FSFree,
        .digest = FSDigest,
    };

    FSType = RedisModule_CreateDataType(ctx, "redis-fs0", 0, &tm);
    if (FSType == NULL) return REDISMODULE_ERR;

    /* ---- Register commands (Unix names) ---- */

    if (RedisModule_CreateCommand(ctx, "FS.INFO",
        FSInfo_RedisCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.ECHO",
        FSEcho_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.CAT",
        FSCat_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.APPEND",
        FSAppend_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.RM",
        FSRm_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.TOUCH",
        FSTouch_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.MKDIR",
        FSMkdir_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.LS",
        FSLs_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.STAT",
        FSStat_RedisCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.TEST",
        FSTest_RedisCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.CHMOD",
        FSChmod_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.CHOWN",
        FSChown_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.LN",
        FSLn_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.READLINK",
        FSReadlink_RedisCommand, "readonly fast", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.CP",
        FSCp_RedisCommand, "write deny-oom", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.MV",
        FSMv_RedisCommand, "write", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.TREE",
        FSTree_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.FIND",
        FSFind_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "FS.GREP",
        FSGrep_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
