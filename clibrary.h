#ifndef CLIBRARY_H
#define CLIBRARY_H

#include <tiledb/tiledb_experimental.h>

typedef const char cchar_t;

int32_t numOfFragmentsInPath(cchar_t* path, void *data);
int32_t vfsLs(cchar_t* path, void *data);
int32_t vfsLsRecursive(cchar_t* path, size_t path_len, uint64_t size, void *data);
int32_t objectsInPath(cchar_t* path, tiledb_object_t objectType, void *data);

int32_t _num_of_folders_in_path(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

int32_t _vfs_ls(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

int32_t _vfs_ls_recursive(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

int32_t _tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    void* data);

int32_t _tiledb_object_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    void* data);

#endif
