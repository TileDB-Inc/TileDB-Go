#ifndef CLIBRARY_H
#define CLIBRARY_H

#include <tiledb/tiledb.h>

typedef const char cchar_t;

int32_t numOfFragmentsInPath(cchar_t* path, void *data);

TILEDB_EXPORT int32_t _num_of_folders_in_path(
	tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data);

#endif