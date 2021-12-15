
#include "clibrary.h"
#include <stdlib.h>
#include <stdio.h>

TILEDB_EXPORT int32_t _num_of_folders_in_path(
  tiledb_ctx_t* ctx,
  tiledb_vfs_t* vfs,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_vfs_ls(ctx, vfs, path, numOfFragmentsInPath, data);
    return ret_val;
}

TILEDB_EXPORT int32_t _list_of_folders_in_path(
  tiledb_ctx_t* ctx,
  tiledb_vfs_t* vfs,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_vfs_ls(ctx, vfs, path, listOfFoldersInPath, data);
    return ret_val;
}

TILEDB_EXPORT int32_t _tiledb_object_walk(
  tiledb_ctx_t* ctx,
  const char* path,
  tiledb_walk_order_t order,
  void* data) {
    int32_t ret_val = tiledb_object_walk(ctx, path, order, objectsInPath, data);
    return ret_val;
}

TILEDB_EXPORT int32_t _tiledb_object_ls(
  tiledb_ctx_t* ctx,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_object_ls(ctx, path, objectsInPath, data);
    return ret_val;
}