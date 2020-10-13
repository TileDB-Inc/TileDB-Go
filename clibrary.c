
#include "clibrary.h"
#include <stdlib.h>

TILEDB_EXPORT int32_t _num_of_folders_in_path(
	tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    void* data) {
		int32_t ret_val = tiledb_vfs_ls(ctx, vfs, path, numOfFragmentsInPath, data);
        return ret_val;
	}