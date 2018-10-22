package tiledb

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_enum.h>
*/
import "C"

import "reflect"

// ArrayType enum for tiledb arrays
type ArrayType int8

const (
	// TILEDB_DENSE dense array
	TILEDB_DENSE ArrayType = C.TILEDB_DENSE
	// TILEDB_SPARSE dense array
	TILEDB_SPARSE ArrayType = C.TILEDB_SPARSE
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

// ReflectKind returns the reflect kind given a datatype
func (d Datatype) ReflectKind() reflect.Kind {
	switch d {
	case TILEDB_INT8:
		return reflect.Int8
	case TILEDB_INT16:
		return reflect.Int16
	case TILEDB_INT32:
		return reflect.Int32
	case TILEDB_INT64:
		return reflect.Int64
	case TILEDB_UINT8:
		return reflect.Uint8
	case TILEDB_UINT16:
		return reflect.Uint16
	case TILEDB_UINT32:
		return reflect.Uint32
	case TILEDB_UINT64:
		return reflect.Uint64
	case TILEDB_FLOAT32:
		return reflect.Float32
	case TILEDB_FLOAT64:
		return reflect.Float64
	case TILEDB_STRING_ASCII:
		return reflect.Uint8
	case TILEDB_STRING_UTF8:
		return reflect.Uint8
	case TILEDB_STRING_UTF16:
		return reflect.Uint16
	case TILEDB_STRING_UTF32:
		return reflect.Uint32
	case TILEDB_STRING_UCS2:
		return reflect.Uint16
	case TILEDB_STRING_UCS4:
		return reflect.Uint32
	case TILEDB_ANY:
		return reflect.Interface
	default:
		return reflect.Interface
	}
}

// EncryptionType represents different encryption algorithms
type EncryptionType uint8

const (
	// TILEDB_NO_ENCRYPTION No encryption
	TILEDB_NO_ENCRYPTION EncryptionType = C.TILEDB_NO_ENCRYPTION
	// TILEDB_AES_256_GCM AES-256-GCM encryption
	TILEDB_AES_256_GCM EncryptionType = C.TILEDB_AES_256_GCM
)

// FilterType for attribute/coordinates/offsets filters
type FilterType uint8

const (
	// TILEDB_FILTER_NONE No-op filter
	TILEDB_FILTER_NONE FilterType = C.TILEDB_FILTER_NONE
	// TILEDB_FILTER_GZIP Gzip compressor
	TILEDB_FILTER_GZIP FilterType = C.TILEDB_FILTER_GZIP
	// TILEDB_FILTER_ZSTD Zstandard compressor
	TILEDB_FILTER_ZSTD FilterType = C.TILEDB_FILTER_ZSTD
	// TILEDB_FILTER_LZ4 LZ4 compressor
	TILEDB_FILTER_LZ4 FilterType = C.TILEDB_FILTER_LZ4
	// TILEDB_FILTER_RLE Run-length encoding compressor
	TILEDB_FILTER_RLE FilterType = C.TILEDB_FILTER_RLE
	// TILEDB_FILTER_BZIP2 Bzip2 compressor
	TILEDB_FILTER_BZIP2 FilterType = C.TILEDB_FILTER_BZIP2
	// TILEDB_FILTER_DOUBLE_DELTA Double-delta compressor
	TILEDB_FILTER_DOUBLE_DELTA FilterType = C.TILEDB_FILTER_DOUBLE_DELTA
	// TILEDB_FILTER_BIT_WIDTH_REDUCTION Bit width reduction filter.
	TILEDB_FILTER_BIT_WIDTH_REDUCTION FilterType = C.TILEDB_FILTER_BIT_WIDTH_REDUCTION
	// TILEDB_FILTER_BITSHUFFLE Bitshuffle filter.
	TILEDB_FILTER_BITSHUFFLE FilterType = C.TILEDB_FILTER_BITSHUFFLE
	// TILEDB_FILTER_BYTESHUFFLE Byteshuffle filter.
	TILEDB_FILTER_BYTESHUFFLE FilterType = C.TILEDB_FILTER_BYTESHUFFLE
	// TILEDB_FILTER_POSITIVE_DELTA Positive-delta encoding filter.
	TILEDB_FILTER_POSITIVE_DELTA FilterType = C.TILEDB_FILTER_POSITIVE_DELTA
)

// FilterOption for a given filter
type FilterOption uint8

const (
	// TILEDB_COMPRESSION_LEVEL Compression level. Type: `int32_t`.
	TILEDB_COMPRESSION_LEVEL FilterOption = C.TILEDB_COMPRESSION_LEVEL
	// TILEDB_BIT_WIDTH_MAX_WINDOW Max window length for bit width reduction. Type: `uint32_t`.
	TILEDB_BIT_WIDTH_MAX_WINDOW FilterOption = C.TILEDB_BIT_WIDTH_MAX_WINDOW
	// TILEDB_POSITIVE_DELTA_MAX_WINDOW Max window length for positive-delta encoding. Type: `uint32_t`.
	TILEDB_POSITIVE_DELTA_MAX_WINDOW FilterOption = C.TILEDB_POSITIVE_DELTA_MAX_WINDOW
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

// QueryStatus status of a query
type QueryStatus int8

const (
	// TILEDB_FAILED Query failed
	TILEDB_FAILED QueryStatus = C.TILEDB_FAILED
	// TILEDB_COMPLETED Query completed (all data has been read)
	TILEDB_COMPLETED QueryStatus = C.TILEDB_COMPLETED
	// TILEDB_INPROGRESS Query is in progress
	TILEDB_INPROGRESS QueryStatus = C.TILEDB_INPROGRESS
	//TILEDB_INCOMPLETE Query completed (but not all data has been read)
	TILEDB_INCOMPLETE QueryStatus = C.TILEDB_INCOMPLETE
	// TILEDB_UNINITIALIZED Query not initialized.
	TILEDB_UNINITIALIZED QueryStatus = C.TILEDB_UNINITIALIZED
)

// QueryType read or write query
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

// TILEDB_VAR_NUM indicates variable sized attributes for cell values
var TILEDB_VAR_NUM = uint(C.TILEDB_VAR_NUM)

// TILEDB_COORDS A special name indicating the coordinates attribute.
var TILEDB_COORDS = C.GoString(C.TILEDB_COORDS)
