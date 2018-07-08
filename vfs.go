package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// VFSfh is a virtual file system file handler
type VFSfh struct {
	tiledbVFSfh *C.tiledb_vfs_fh_t
	context     *Context
}

// Free a tiledb c vfs file handler
func (v *VFSfh) Free() {
	if v.tiledbVFSfh != nil {
		C.tiledb_vfs_fh_free(&v.tiledbVFSfh)
	}
}

// IsClosed checks a vfs file handler to see if it is closed. Return true if
// file handler is closed, false if its not closed and error is non-nil on error
func (v *VFSfh) IsClosed() (bool, error) {
	var isClosed C.int

	ret := C.tiledb_vfs_fh_is_closed(v.context.tiledbContext, v.tiledbVFSfh, &isClosed)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking if vfs file handler is closed")
	}

	if isClosed == 1 {
		return true, nil
	}

	return false, nil
}

// VFS Implements a virtual filesystem that enables performing directory/file
// operations with a unified API on different filesystems, such as local
// posix/windows, HDFS, AWS S3, etc.
type VFS struct {
	tiledbVFS *C.tiledb_vfs_t
	context   *Context
}

// NewVFS alloc a new context using tiledb_vfs_alloc. This also registers the
// `runtime.SetFinalizer` for handling the free'ing of the c data structure on
// garbage collection
func NewVFS(context *Context, config *Config) (*VFS, error) {
	vfs := VFS{context: context}
	var err *C.tiledb_error_t
	C.tiledb_vfs_alloc(context.tiledbContext, config.tiledbConfig, &vfs.tiledbVFS)
	if err != nil {
		var msg *C.char
		defer C.free(unsafe.Pointer(msg))
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("Error creating tiledb context: %s", C.GoString(msg))
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&vfs, func(vfs *VFS) {
		vfs.Free()
	})

	return &vfs, nil
}

// Free tiledb_vfs_t c structure that was allocated on the heap
func (v *VFS) Free() {
	if v.tiledbVFS != nil {
		C.tiledb_vfs_free(&v.tiledbVFS)
	}
}

// CreateBucket creates an object-store bucket with the input URI.
func (v *VFS) CreateBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_create_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in creating s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// RemoveBucket deletes an object-store bucket with the input URI.
func (v *VFS) RemoveBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_remove_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in removing s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// EmptyBucket empty a bucket
func (v *VFS) EmptyBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_empty_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in emptying s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsEmptyBucket check if a bucket is empty
func (v *VFS) IsEmptyBucket(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isEmpty C.int
	ret := C.tiledb_vfs_is_empty_bucket(v.context.tiledbContext, v.tiledbVFS, curi, &isEmpty)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking if s3 bucket %s is empty: %s", uri, v.context.LastError())
	}

	if isEmpty == 1 {
		return true, nil
	}

	return false, nil
}

// IsBucket checks if an object-store bucket with the input URI exists.
func (v *VFS) IsBucket(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isBucket C.int
	ret := C.tiledb_vfs_is_bucket(v.context.tiledbContext, v.tiledbVFS, curi, &isBucket)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking if %s is a s3 bucket: %s", uri, v.context.LastError())
	}

	if isBucket == 1 {
		return true, nil
	}

	return false, nil
}

// CreateDir creates a directory with the input URI.
func (v *VFS) CreateDir(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_create_dir(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in creating directory %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsDir checks if a directory with the input URI exists.
func (v *VFS) IsDir(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isDir C.int
	ret := C.tiledb_vfs_is_dir(v.context.tiledbContext, v.tiledbVFS, curi, &isDir)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking if %s is a directory: %s", uri, v.context.LastError())
	}

	if isDir == 1 {
		return true, nil
	}

	return false, nil
}

// RemoveDir removes a directory (recursively) with the input URI.
func (v *VFS) RemoveDir(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_remove_dir(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in removing directory %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsFile checks if a file with the input URI exists.
func (v *VFS) IsFile(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isFile C.int
	ret := C.tiledb_vfs_is_file(v.context.tiledbContext, v.tiledbVFS, curi, &isFile)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error in checking if %s is a file: %s", uri, v.context.LastError())
	}

	if isFile == 1 {
		return true, nil
	}

	return false, nil
}

// RemoveFile deletes a file with the input URI.
func (v *VFS) RemoveFile(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_remove_file(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in removing file %s: %s", uri, v.context.LastError())
	}

	return nil
}

// FileSize retrieves the size of a file.
func (v *VFS) FileSize(uri string) (uint64, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var cfsize C.uint64_t
	ret := C.tiledb_vfs_file_size(v.context.tiledbContext, v.tiledbVFS, curi, &cfsize)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error in removing file %s: %s", uri, v.context.LastError())
	}

	return uint64(cfsize), nil
}

// MoveFile renames a TileDB file from an old URI to a new URI.
func (v *VFS) MoveFile(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_move_file(v.context.tiledbContext, v.tiledbVFS, cOldURI, cNewURI)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in moving file %s to %s: %s", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// MoveDir menames a TileDB directory from an old URI to a new URI.
func (v *VFS) MoveDir(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_move_dir(v.context.tiledbContext, v.tiledbVFS, cOldURI, cNewURI)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in moving directory %s to %s: %s", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// Open prepares a file for reading/writing.
func (v *VFS) Open(uri string, mode VFSMode) (*VFSfh, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	fh := &VFSfh{context: v.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(fh, func(fh *VFSfh) {
		fh.Free()
	})

	ret := C.tiledb_vfs_open(v.context.tiledbContext, v.tiledbVFS, curi, C.tiledb_vfs_mode_t(mode), &fh.tiledbVFSfh)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("Out of Memory error in VFS.Open: %s", v.context.LastError())
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Unknown error in VFS.Open: %s", v.context.LastError())
	}

	return fh, nil
}

// Close a file. This is flushes the buffered data into the file when the file
// was opened in write (or append) mode. It is particularly important to be
// called after S3 writes, as otherwise the writes will not take effect.
func (v *VFS) Close(fh *VFSfh) error {

	ret := C.tiledb_vfs_close(v.context.tiledbContext, fh.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Unknown error in VFS.Close: %s", v.context.LastError())
	}

	fh.Free()
	return nil
}

// Read part of a file
func (v *VFS) Read(fh *VFSfh, offset uint64, nbytes uint64) ([]byte, error) {
	bytes := make([]byte, nbytes)
	cbuffer := C.CBytes(bytes)
	ret := C.tiledb_vfs_read(v.context.tiledbContext, fh.tiledbVFSfh, C.uint64_t(offset), cbuffer, C.uint64_t(nbytes))

	if ret != C.TILEDB_OK {
		return []byte{}, fmt.Errorf("Unknown error in VFS.Read: %s", v.context.LastError())
	}

	bytes = C.GoBytes(cbuffer, C.int(nbytes))

	return bytes, nil
}

// Write the contents of a buffer into a file. Note that this function only
// appends data at the end of the file. If the file does not exist,
// it will be created
func (v *VFS) Write(fh *VFSfh, bytes []byte) error {
	cbuffer := C.CBytes(bytes)
	ret := C.tiledb_vfs_write(v.context.tiledbContext, fh.tiledbVFSfh, cbuffer, C.uint64_t(len(bytes)))

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Unknown error in VFS.Write: %s", v.context.LastError())
	}

	return nil
}

// Sync (flushes) a file.
func (v *VFS) Sync(fh *VFSfh) error {
	ret := C.tiledb_vfs_sync(v.context.tiledbContext, fh.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Unknown error in VFS.Sync: %s", v.context.LastError())
	}

	return nil
}

// Touch a file, i.e., creates a new empty file.
func (v *VFS) Touch(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_touch(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in touching %s: %s", uri, v.context.LastError())
	}

	return nil
}
