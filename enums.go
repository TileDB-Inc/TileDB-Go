package tiledb

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_enum.h>
*/
import "C"

// ArrayType enum for tiledb arrays
type ArrayType int8

const (
	// TILEDB_DENSE dense array
	TILEDB_DENSE ArrayType = C.TILEDB_DENSE
	// TILEDB_SPARSE dense array
	TILEDB_SPARSE ArrayType = C.TILEDB_SPARSE
)

// CompressorType enum in tiledb for compressors
type CompressorType int8

const (
	// TILEDB_NO_COMPRESSION No compressor
	TILEDB_NO_COMPRESSION CompressorType = C.TILEDB_NO_COMPRESSION
	// TILEDB_GZIP Gzip compressor
	TILEDB_GZIP CompressorType = C.TILEDB_GZIP
	// TILEDB_ZSTD Zstandard compressor
	TILEDB_ZSTD CompressorType = C.TILEDB_ZSTD
	// TILEDB_LZ4 LZ4 compressor
	TILEDB_LZ4 CompressorType = C.TILEDB_LZ4
	// TILEDB_BLOSC_LZ Blosc compressor using LZ
	TILEDB_BLOSC_LZ CompressorType = C.TILEDB_BLOSC_LZ
	// TILEDB_BLOSC_LZ4 Blosc compressor using LZ4
	TILEDB_BLOSC_LZ4 CompressorType = C.TILEDB_BLOSC_LZ4
	// TILEDB_BLOSC_LZ4HC Blosc compressor using LZ4HC
	TILEDB_BLOSC_LZ4HC CompressorType = C.TILEDB_BLOSC_LZ4HC
	// TILEDB_BLOSC_SNAPPY Blosc compressor using Snappy
	TILEDB_BLOSC_SNAPPY CompressorType = C.TILEDB_BLOSC_SNAPPY
	// TILEDB_BLOSC_ZLIB Blosc compressor using zlib
	TILEDB_BLOSC_ZLIB CompressorType = C.TILEDB_BLOSC_ZLIB
	// TILEDB_BLOSC_ZSTD Blosc compressor using Zstandard
	TILEDB_BLOSC_ZSTD CompressorType = C.TILEDB_BLOSC_ZSTD
	// TILEDB_RLE Run-length encoding compressor
	TILEDB_RLE CompressorType = C.TILEDB_RLE
	// TILEDB_BZIP2 Bzip2 compressor
	TILEDB_BZIP2 CompressorType = C.TILEDB_BZIP2
	// TILEDB_DOUBLE_DELTA Double-delta compressor
	TILEDB_DOUBLE_DELTA CompressorType = C.TILEDB_DOUBLE_DELTA
)

// Datatype
type Datatype int8

const (
	// TILEDB_INT32 32-bit signed integer
	TILEDB_INT32 Datatype = C.TILEDB_INT32
	// TILEDB_INT64 64-bit signed integer
	TILEDB_INT64 Datatype = C.TILEDB_INT64
	// TILEDB_FLOAT32 32-bit floating point value
	TILEDB_FLOAT32 Datatype = C.TILEDB_FLOAT32
	// TILEDB_FLOAT64 64-bit floating point value
	TILEDB_FLOAT64 Datatype = C.TILEDB_FLOAT64
	// TILEDB_CHAR Character
	TILEDB_CHAR Datatype = C.TILEDB_CHAR
	// TILEDB_INT8 8-bit signed integer
	TILEDB_INT8 Datatype = C.TILEDB_INT8
	// TILEDB_UINT8 8-bit unsigned integer
	TILEDB_UINT8 Datatype = C.TILEDB_UINT8
	// TILEDB_INT16 16-bit signed integer
	TILEDB_INT16 Datatype = C.TILEDB_INT16
	// TILEDB_UINT16 16-bit unsigned integer
	TILEDB_UINT16 Datatype = C.TILEDB_UINT16
	// TILEDB_UINT32 32-bit unsigned integer
	TILEDB_UINT32 Datatype = C.TILEDB_UINT32
	// TILEDB_UINT64 64-bit unsigned integer
	TILEDB_UINT64 Datatype = C.TILEDB_UINT64
	// TILEDB_STRING_ASCII ASCII string
	TILEDB_STRING_ASCII Datatype = C.TILEDB_STRING_ASCII
	// TILEDB_STRING_UTF8 UTF-8 string
	TILEDB_STRING_UTF8 Datatype = C.TILEDB_STRING_UTF8
	// TILEDB_STRING_UTF16 UTF-16 string
	TILEDB_STRING_UTF16 Datatype = C.TILEDB_STRING_UTF16
	// TILEDB_STRING_UTF32 UTF-32 string
	TILEDB_STRING_UTF32 Datatype = C.TILEDB_STRING_UTF32
	// TILEDB_STRING_UCS2 UCS2 string
	TILEDB_STRING_UCS2 Datatype = C.TILEDB_STRING_UCS2
	// TILEDB_STRING_UCS4 UCS4 string
	TILEDB_STRING_UCS4 Datatype = C.TILEDB_STRING_UCS4
	// TILEDB_ANY This can be any datatype. Must store (type tag, value) pairs.
	TILEDB_ANY Datatype = C.TILEDB_ANY
)

// FS represents support fs types
type FS int8

const (
	// TILEDB_HDFS HDFS filesystem support
	TILEDB_HDFS FS = C.TILEDB_HDFS

	// TILEDB_S3 S3 filesystem support
	TILEDB_S3 FS = C.TILEDB_S3
)

// Layout cell/tile layout
type Layout int8

const (
	// TILEDB_ROW_MAJOR Row-major layout
	TILEDB_ROW_MAJOR Layout = C.TILEDB_ROW_MAJOR
	// TILEDB_COL_MAJOR Column-major layout
	TILEDB_COL_MAJOR Layout = C.TILEDB_COL_MAJOR
	// TILEDB_GLOBAL_ORDER Global-order layout
	TILEDB_GLOBAL_ORDER Layout = C.TILEDB_GLOBAL_ORDER
	// TILEDB_UNORDERED Unordered layout
	TILEDB_UNORDERED Layout = C.TILEDB_UNORDERED
)

type QueryType int8

const (
	// TILEDB_READ Read query
	TILEDB_READ QueryType = C.TILEDB_READ
	// TILEDB_WRITE Write query
	TILEDB_WRITE QueryType = C.TILEDB_WRITE
)

// VFSMode is virtual file system file open mode
type VFSMode int8

const (
	// TILEDB_VFS_READ open file in read mode
	TILEDB_VFS_READ VFSMode = C.TILEDB_VFS_READ

	// TILEDB_VFS_WRITE open file in write mode
	TILEDB_VFS_WRITE VFSMode = C.TILEDB_VFS_WRITE

	// TILEDB_VFS_APPENDopen file in write append mode
	TILEDB_VFS_APPEND VFSMode = C.TILEDB_VFS_APPEND
)

// TIELDB_VAR_NUM indicates variable sized attributes for cell values
var TILEDB_VAR_NUM = uint(C.TILEDB_VAR_NUM)
