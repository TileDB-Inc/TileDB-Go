
#include "clibrary.h"
#include <stdlib.h>
#include <stdio.h>

int32_t _num_of_folders_in_path(
  tiledb_ctx_t* ctx,
  tiledb_vfs_t* vfs,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_vfs_ls(ctx, vfs, path, numOfFragmentsInPath, data);
    return ret_val;
}

int32_t _vfs_ls(
  tiledb_ctx_t* ctx,
  tiledb_vfs_t* vfs,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_vfs_ls(ctx, vfs, path, vfsLs, data);
    return ret_val;
}

int32_t _vfs_ls_recursive(
  tiledb_ctx_t* ctx,
  tiledb_vfs_t* vfs,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_vfs_ls_recursive(ctx, vfs, path, vfsLsRecursive, data);
    return ret_val;
}

int32_t _tiledb_object_walk(
  tiledb_ctx_t* ctx,
  const char* path,
  tiledb_walk_order_t order,
  void* data) {
    int32_t ret_val = tiledb_object_walk(ctx, path, order, objectsInPath, data);
    return ret_val;
}

int32_t _tiledb_object_ls(
  tiledb_ctx_t* ctx,
  const char* path,
  void* data) {
    int32_t ret_val = tiledb_object_ls(ctx, path, objectsInPath, data);
    return ret_val;
}
