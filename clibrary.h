#ifndef CLIBRARY_H
#define CLIBRARY_H

#include <tiledb/tiledb.h>

typedef const char cchar_t;

int32_t numOfFragmentsInPath(cchar_t* path, void *data);
int32_t listOfFoldersInPath(cchar_t* path, void *data);
int32_t objectsInPath(cchar_t* path, tiledb_object_t objectType, void *data);

TILEDB_EXPORT int32_t _num_of_folders_in_path(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

TILEDB_EXPORT int32_t _list_of_folders_in_path(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

TILEDB_EXPORT int32_t _tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    void* data);

TILEDB_EXPORT int32_t _tiledb_object_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    void* data);

#endif