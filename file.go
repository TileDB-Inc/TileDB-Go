//go:build experimental
// +build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb_experimental.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

type File struct {
	tiledbFile *C.tiledb_file_t
	context    *Context
	config     *Config
	uri        string
}

// NewFile creates a TileDB file object.
func NewFile(tdbCtx *Context, uri string) (*File, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	file := File{context: tdbCtx, uri: uri}
	ret := C.tiledb_file_alloc(file.context.tiledbContext, curi, &file.tiledbFile)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb file: %s",
			file.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&file,
		func(file *File) {
			file.Free()
		})

	return &file, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (f *File) Free() {
	if f.tiledbFile != nil {
		C.tiledb_file_free(&f.tiledbFile)
	}
}

// SetConfig sets config on file
func (f *File) SetConfig(config *Config) error {
	f.config = config

	ret := C.tiledb_file_set_config(f.context.tiledbContext, f.tiledbFile, f.config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting config on file: %s", f.context.LastError())
	}

	return nil
}

//// Config gets config from file
//func (f *File) Config() (*Config, error) {
//	config := Config{}
//	ret := C.tiledb_file_get_config(f.context.tiledbContext, f.tiledbFile, &config.tiledbConfig)
//	if ret != C.TILEDB_OK {
//		return nil, fmt.Errorf("error getting config from file: %s", f.context.LastError())
//	}
//
//	runtime.SetFinalizer(&config, func(config *Config) {
//		config.Free()
//	})
//
//	if f.config == nil {
//		f.config = &config
//	}
//
//	return &config, nil
//}

// CreateDefault creates a file array with default schema
func (f *File) CreateDefault() error {
	if f.config == nil {
		return fmt.Errorf("error creating file with default schema: missing config")
	}

	ret := C.tiledb_file_create_default(f.context.tiledbContext, f.tiledbFile, f.config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error creating file with default schema: %s", f.context.LastError())
	}

	return nil
}

/**
 * Create a file array using heuristics based on a file at provided URI
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_create_from_uri(ctx, file, "input_file", NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param input_uri URI to read file from
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_create_from_uri(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    const char* input_uri,
//    tiledb_config_t* config);

/**
 * Create a file array using heuristics based on a file from provided VFS
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 *
 * tiledb_vfs_t vfs*;
 * tiledb_vfs_alloc(ctx, &vfs);
 * tiledb_vfs_fh_t *vfs_fh;
 * tiledb_vfs_open(ctx, vfs, "some_file", TILEDB_VFS_READ, &fh);
 * tiledb_file_create_from_vfs_fh(ctx, file, vfs_fh, NULL);
 *
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param input vfs file handle to create from.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_create_from_vfs_fh(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    tiledb_vfs_fh_t* input,
//    tiledb_config_t* config);

/**
 * Read a file into the file array from the given FILE handle
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param in FILE handle to read from
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_store_fh(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* in, tiledb_config_t* config);

/**
 * Store raw bytes from byte array into file array
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param bytes
 * @param size
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_store_buffer(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    void* bytes,
//    uint64_t size,
//    tiledb_config_t* config);

/**
 * Store file from URI into file array
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_store_uri(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    const char*,
//    tiledb_config_t* config);

/**
 * Store file from VFS File Handle into file array
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param config TileDB Config for setting to create.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_store_vfs_fh(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    tiledb_vfs_fh_t*,
//    tiledb_config_t* config);

/**
 * Get the file MIME type
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_type;
 * uint32_t size = 0;
 * tiledb_file_get_mime_type(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param mime_type char* to set to mime_type
 * @param size length of mime string
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_mime_type(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    const char** mime_type,
//    uint32_t size);

/**
 * Get the file MIME encoding
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * const char* mime_encoding;
 * uint32_t size = 0;
 * tiledb_file_get_mime_encoding(ctx, file, *mime_type, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param mime_type char* to set to mime encoding
 * @param size length of mime string
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_mime_encoding(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    const char** mime_type,
//    uint32_t size);

/**
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* name;
 * uint32_t size = 0;
 * tiledb_file_get_original_name(ctx, file, &name, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_original_name(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** name, uint32_t* size);

/**
 *
 *  * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* ext;
 * uint32_t size = 0;
 * tiledb_file_get_extension(ctx, file, &name, &size);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_extension(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** ext, uint32_t* size);

/**
 * Get Array Schema from file
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_array_schema_t* schema;
 * tiledb_file_get_schema(ctx, file, &schema);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param array_schema
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_schema(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    tiledb_array_schema_t** array_schema);

/**
 * Export a file to a raw buffer
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * const char* file_uri = "some_file";
 * FILE* file_out = fopen(file_uri, "w+");
 * tiledb_file_export_fh(ctx, file, file_out, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param out FILE handle to write to
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_export_fh(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* out, tiledb_config_t* config);

/**
 * Export a file to a raw buffer
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t size = 0;
 * uint64_t file_offset = 0;
 * tiledb_file_get_size(ctx, file, &size);
 * void* buffer = malloc(size);
 * tiledb_file_export_buffer(ctx, file, bytes, &size, offset, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param bytes output buffer, alloc'ed by used.
 * @param size size to read
 * @param file_offset file_offset to read from
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_export_buffer(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    void* bytes,
//    uint64_t* size,
//    uint64_t file_offset,
//    tiledb_config_t* config);

/**
 * Export a file to the provided URI
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_file_export_uri(ctx, file, "s3://tiledb_bucket/some_output_file",
 * NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param output_uri output uri to save to
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_export_uri(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    const char* output_uri,
//    tiledb_config_t* config);

/**
 * Export a file to the opened VFS file handle
 *
 * * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * tiledb_vfs_t vfs*;
 * tiledb_vfs_alloc(ctx, &vfs);
 * tiledb_vfs_fh_t *vfs_fh;
 * tiledb_vfs_open(ctx, vfs, "some_file", TILEDB_VFS_WRITE, &fh);
 * tiledb_file_export_vfs_fh(ctx, file, output, NULL);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param output output vfs file handle
 * @param config TileDB Config object for export settings
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_export_vfs_fh(
//    tiledb_ctx_t* ctx,
//    tiledb_file_t* file,
//    tiledb_vfs_fh_t* output,
//    tiledb_config_t* config);

/**
 * Sets the starting timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The default value is `0`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_start The epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_start(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_start);

/**
 * Sets the ending timestamp to use when opening (and reopening) the file.
 * This is an inclusive bound. The UINT64_MAX timestamp is a reserved timestamp
 * that will be interpretted as the current timestamp when an file is opened.
 * The default value is `UINT64_MAX`.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_end The epoch timestamp in milliseconds. Use UINT64_MAX for
 *   the current timestamp.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_set_open_timestamp_end(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_end);

/**
 * Gets the starting timestamp used when opening (and reopening) the file.
 * This is an inclusive bound.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_start(ctx, file, 1234);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_start;
 * tiledb_file_get_open_timestamp_start(ctx, file, &timestamp_start);
 * assert(timestamp_start == 1234);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_start The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_start(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_start);

/**
 * Gets the ending timestamp used when opening (and reopening) the file.
 * This is an inclusive bound. If UINT64_MAX was set, this will return
 * the timestamp at the time the file was opened. If the file has not
 * yet been opened, it will return UINT64_MAX.`
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "s3://tiledb_bucket/my_file", &file);
 * tiledb_file_set_open_timestamp_end(ctx, file, 5678);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 *
 * uint64_t timestamp_end;
 * tiledb_file_get_open_timestamp_end(ctx, file, &timestamp_end);
 * assert(timestamp_start == 5678);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object.
 * @param timestamp_end The output epoch timestamp in milliseconds.
 * @return `TILEDB_OK` for success or `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t tiledb_file_get_open_timestamp_end(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_end);

/**
 * Opens a TileDB file. The file is opened using a query type as input.
 * This is to indicate that queries created for this `tiledb_file_t`
 * object will inherit the query type. In other words, `tiledb_file_t`
 * objects are opened to receive only one type of queries.
 * They can always be closed and be re-opened with another query type.
 * Also there may be many different `tiledb_file_t`
 * objects created and opened with different query types.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object to be opened.
 * @param query_type The type of queries the file object will be receiving.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the same file object is opened again without being closed,
 *     an error will be thrown.
 * @note The config should be set before opening an file.
 * @note If the file is to be opened at a specfic time interval, the
 *      `timestamp{start, end}` values should be set to a config that's set to
 *       the file object before opening the file.
 */
//TILEDB_EXPORT int32_t tiledb_file_open(
//    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type);

/**
 * Closes a TileDB file.
 *
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * tiledb_file_close(ctx, file);
 * @endcode
 *
 * @param ctx The TileDB context.
 * @param file The file object to be closed.
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 *
 * @note If the file object has already been closed, the function has
 *     no effect.
 */
//TILEDB_EXPORT int32_t tiledb_file_close(tiledb_ctx_t* ctx, tiledb_file_t* file);

/**
 * Get the size of the opened file
 * **Example:**
 *
 * @code{.c}
 * tiledb_file_t* file;
 * tiledb_file_alloc(ctx, "hdfs:///tiledb_files/my_file", &file);
 * tiledb_file_open(ctx, file, TILEDB_READ);
 * uint64_t size = 0;
 * tiledb_file_get_size(ctx, file, &size);
 * tiledb_file_close(ctx, file);
 * @endcode
 *
 * @param ctx  The TileDB context.
 * @param file The file object to be closed.
 * @param size of the file
 * @return `TILEDB_OK` for success and `TILEDB_ERR` for error.
 */
//TILEDB_EXPORT int32_t
//tiledb_file_get_size(tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* size);
