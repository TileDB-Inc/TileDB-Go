package tiledb

/*
#cgo CFLAGS: -I/usr/local/include
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_enum.h>
*/
import "C"

// TiledbFS
type TiledbFS int8

const (
	// TILEDB_HDFS HDFS filesystem support
	TILEDB_HDFS TiledbFS = C.TILEDB_HDFS

	// TILEDB_S3 S3 filesystem support
	TILEDB_S3 TiledbFS = C.TILEDB_S3
)

type TiledbVFSMode int8

const (
	// TILEDB_VFS_READ open file in read mode
	TILEDB_VFS_READ TiledbVFSMode = C.TILEDB_VFS_READ

	// TILEDB_VFS_WRITE open file in write mode
	TILEDB_VFS_WRITE TiledbVFSMode = C.TILEDB_VFS_WRITE

	// TILEDB_VFS_APPENDopen file in write append mode
	TILEDB_VFS_APPEND TiledbVFSMode = C.TILEDB_VFS_APPEND
)
