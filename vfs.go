package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
#include "clibrary.h"
*/
import "C"

import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"unsafe"

	pointer "github.com/mattn/go-pointer"
)

const arrayMetadataFolderName = "__meta"

// VFSfh is a virtual file system file handler
type VFSfh struct {
	vfs         *VFS
	tiledbVFSfh *C.tiledb_vfs_fh_t
	context     *Context
	offset      uint64
	size        *uint64
	uri         string
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (v *VFSfh) Free() {
	if v.tiledbVFSfh != nil {
		C.tiledb_vfs_fh_free(&v.tiledbVFSfh)
	}
}

// Context exposes the internal TileDB context used to initialize the vfsh
func (v *VFSfh) Context() *Context {
	return v.context
}

// IsClosed checks a vfs file handler to see if it is closed. Return true if
// file handler is closed, false if its not closed and error is non-nil on error
func (v *VFSfh) IsClosed() (bool, error) {
	var isClosed C.int32_t

	ret := C.tiledb_vfs_fh_is_closed(v.context.tiledbContext, v.tiledbVFSfh, &isClosed)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if vfs file handler is closed")
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
		C.tiledb_error_message(err, &msg)
		defer C.tiledb_error_free(&err)
		return nil, fmt.Errorf("error creating tiledb context: %s", C.GoString(msg))
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&vfs, func(vfs *VFS) {
		vfs.Free()
	})

	return &vfs, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (v *VFS) Free() {
	if v.tiledbVFS != nil {
		C.tiledb_vfs_free(&v.tiledbVFS)
	}
}

// Context exposes the internal TileDB context used to initialize the vfs
func (v *VFS) Context() *Context {
	return v.context
}

// Config retrieves a copy of the config from vfs
func (v *VFS) Config() (*Config, error) {
	var config Config
	ret := C.tiledb_vfs_get_config(v.context.tiledbContext, v.tiledbVFS,
		&config.tiledbConfig)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("out of Memory error in GetConfig")
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("unknown error in GetConfig")
	}

	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	return &config, nil
}

// CreateBucket creates an object-store bucket with the input URI.
func (v *VFS) CreateBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_create_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in creating s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// RemoveBucket deletes an object-store bucket with the input URI.
func (v *VFS) RemoveBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_remove_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in removing s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// EmptyBucket empty a bucket
func (v *VFS) EmptyBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_empty_bucket(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in emptying s3 bucket %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsEmptyBucket check if a bucket is empty
func (v *VFS) IsEmptyBucket(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isEmpty C.int32_t
	ret := C.tiledb_vfs_is_empty_bucket(v.context.tiledbContext, v.tiledbVFS, curi, &isEmpty)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if s3 bucket %s is empty: %s", uri, v.context.LastError())
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
	var isBucket C.int32_t
	ret := C.tiledb_vfs_is_bucket(v.context.tiledbContext, v.tiledbVFS, curi, &isBucket)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if %s is a s3 bucket: %s", uri, v.context.LastError())
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
		return fmt.Errorf("error in creating directory %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsDir checks if a directory with the input URI exists.
func (v *VFS) IsDir(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isDir C.int32_t
	ret := C.tiledb_vfs_is_dir(v.context.tiledbContext, v.tiledbVFS, curi, &isDir)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if %s is a directory: %s", uri, v.context.LastError())
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
		return fmt.Errorf("error in removing directory %s: %s", uri, v.context.LastError())
	}

	return nil
}

// IsFile checks if a file with the input URI exists.
func (v *VFS) IsFile(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isFile C.int32_t
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
		return fmt.Errorf("error in removing file %s: %s", uri, v.context.LastError())
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
		return 0, fmt.Errorf("error in getting file size %s: %s", uri, v.context.LastError())
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
		return fmt.Errorf("error in moving file %s to %s: %s", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// CopyFile renames a TileDB file from an old URI to a new URI.
func (v *VFS) CopyFile(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_copy_file(v.context.tiledbContext, v.tiledbVFS, cOldURI, cNewURI)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in copying file %s to %s: %s", oldURI, newURI, v.context.LastError())
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
		return fmt.Errorf("error in moving directory %s to %s: %s", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// Open prepares a file for reading/writing.
func (v *VFS) Open(uri string, mode VFSMode) (*VFSfh, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	fh := &VFSfh{context: v.context, uri: uri, vfs: v}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(fh, func(fh *VFSfh) {
		fh.Free()
	})

	ret := C.tiledb_vfs_open(v.context.tiledbContext, v.tiledbVFS, curi, C.tiledb_vfs_mode_t(mode), &fh.tiledbVFSfh)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("out of Memory error in VFS.Open: %s", v.context.LastError())
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("unknown error in VFS.Open: %s", v.context.LastError())
	}

	return fh, nil
}

// Close a file. This is flushes the buffered data into the file when the file
// was opened in write (or append) mode. It is particularly important to be
// called after S3 writes, as otherwise the writes will not take effect.
func (v *VFS) Close(fh *VFSfh) error {

	ret := C.tiledb_vfs_close(v.context.tiledbContext, fh.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Close: %s", v.context.LastError())
	}

	fh.Free()
	return nil
}

// Read part of a file
func (v *VFS) Read(fh *VFSfh, offset uint64, nbytes uint64) ([]byte, error) {
	bytes := make([]byte, nbytes)
	cbuffer := unsafe.Pointer(&bytes[0])
	ret := C.tiledb_vfs_read(v.context.tiledbContext, fh.tiledbVFSfh, C.uint64_t(offset), cbuffer, C.uint64_t(nbytes))

	if ret != C.TILEDB_OK {
		return []byte{}, fmt.Errorf("unknown error in VFS.Read: %s", v.context.LastError())
	}

	return bytes, nil
}

// Write the contents of a buffer into a file. Note that this function only
// appends data at the end of the file. If the file does not exist,
// it will be created
func (v *VFS) Write(fh *VFSfh, bytes []byte) error {
	cbuffer := C.CBytes(bytes)
	ret := C.tiledb_vfs_write(v.context.tiledbContext, fh.tiledbVFSfh, cbuffer, C.uint64_t(len(bytes)))

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Write: %s", v.context.LastError())
	}

	return nil
}

// Sync (flushes) a file.
func (v *VFS) Sync(fh *VFSfh) error {
	ret := C.tiledb_vfs_sync(v.context.tiledbContext, fh.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Sync: %s", v.context.LastError())
	}

	return nil
}

// Touch a file, i.e., creates a new empty file.
func (v *VFS) Touch(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_touch(v.context.tiledbContext, v.tiledbVFS, curi)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in touching %s: %s", uri, v.context.LastError())
	}

	return nil
}

// DirSize retrieves the size of a directory.
func (v *VFS) DirSize(uri string) (uint64, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var cfsize C.uint64_t
	ret := C.tiledb_vfs_dir_size(v.context.tiledbContext, v.tiledbVFS, curi, &cfsize)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error in getting dir size %s: %s", uri, v.context.LastError())
	}

	return uint64(cfsize), nil
}

// NumOfFragmentsData is a type
type NumOfFragmentsData struct {
	NumOfFolders int
	Vfs          *VFS
}

// FolderData is a type
type FolderData struct {
	Folders []string
	Vfs     *VFS
}

//export numOfFragmentsInPath
func numOfFragmentsInPath(path *C.cchar_t, data unsafe.Pointer) int32 {
	numOfFragmentsData := pointer.Restore(data).(*NumOfFragmentsData)

	uri := C.GoString(path)

	isDir, err := numOfFragmentsData.Vfs.IsDir(uri)
	if err != nil {
		return 0
	}

	if isDir && !strings.HasSuffix(uri, arrayMetadataFolderName) {
		numOfFragmentsData.NumOfFolders++
	}

	return 1
}

//export listOfFoldersInPath
func listOfFoldersInPath(path *C.cchar_t, data unsafe.Pointer) int32 {
	folderData := pointer.Restore(data).(*FolderData)

	uri := C.GoString(path)

	isDir, err := folderData.Vfs.IsDir(uri)
	if err != nil {
		return 0
	}

	if isDir {
		folderData.Folders = append(folderData.Folders, uri)
	}

	return 1
}

// NumOfFragmentsInPath returns number of folders in a path
func (v *VFS) NumOfFragmentsInPath(path string) (int, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	numOfFragmentsData := NumOfFragmentsData{
		NumOfFolders: 0,
		Vfs:          v,
	}
	data := pointer.Save(&numOfFragmentsData)

	ret := C._num_of_folders_in_path(v.context.tiledbContext, v.tiledbVFS, cpath, data)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error in getting dir list %s: %s", path, v.context.LastError())
	}

	return numOfFragmentsData.NumOfFolders, nil
}

// Close a file. This is flushes the buffered data into the file when the file
// was opened in write (or append) mode. It is particularly important to be
// called after S3 writes, as otherwise the writes will not take effect.
func (v *VFSfh) Close() error {

	ret := C.tiledb_vfs_close(v.context.tiledbContext, v.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Close: %s", v.context.LastError())
	}

	v.Free()
	return nil
}

// Read part of a file
func (v *VFSfh) Read(p []byte) (int, error) {
	nbytes := uint64(len(p))

	// If the size is empty, fetch it
	if v.size == nil {
		err := v.fetchAndSetSize()
		if err != nil {
			return 0, err
		}
	}

	// If the requested read size is beyond the limit, adjust bytes to read
	if v.offset+nbytes > *v.size {
		nbytes = *v.size - v.offset
	}

	if nbytes == 0 {
		return 0, io.EOF
	}

	cbuffer := unsafe.Pointer(&p[0])
	ret := C.tiledb_vfs_read(v.context.tiledbContext, v.tiledbVFSfh, C.uint64_t(v.offset), cbuffer, C.uint64_t(nbytes))

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("unknown error in VFS.Read: %s", v.context.LastError())
	}

	v.offset += nbytes
	return int(nbytes), nil
}

// Write the contents of a buffer into a file. Note that this function only
// appends data at the end of the file. If the file does not exist,
// it will be created
func (v *VFSfh) Write(bytes []byte) (int, error) {
	cbuffer := unsafe.Pointer(&bytes[0])
	ret := C.tiledb_vfs_write(v.context.tiledbContext, v.tiledbVFSfh, cbuffer, C.uint64_t(len(bytes)))

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("unknown error in VFS.Write: %s", v.context.LastError())
	}

	return len(bytes), nil
}

// Sync (flushes) a file.
func (v *VFSfh) Sync() error {
	ret := C.tiledb_vfs_sync(v.context.tiledbContext, v.tiledbVFSfh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Sync: %s", v.context.LastError())
	}

	return nil
}

// Seek to an offset
func (v *VFSfh) Seek(offset int64, whence int) (int64, error) {
	absOffset := uint64(offset)
	if offset <= 0 {
		absOffset = uint64(-1 * offset)
	}
	var origin uint64

	switch whence {
	case io.SeekStart:
		origin = 0
	case io.SeekCurrent:
		origin = v.offset
	case io.SeekEnd:
		// If the size is empty, fetch it
		if v.size == nil {
			err := v.fetchAndSetSize()
			if err != nil {
				return -1, err
			}
		}
		origin = *v.size
	default:
		return -1, fmt.Errorf("unknown seek whence")
	}

	if (offset < 0 && absOffset > origin) ||
		(offset >= 0 && absOffset > *v.size-origin) {
		return -1, fmt.Errorf("invalid offset")
	}

	v.offset = uint64(int64(origin) + offset)
	return int64(v.offset), nil
}

func (v *VFSfh) fetchAndSetSize() error {
	size, err := v.vfs.FileSize(v.uri)
	if err != nil {
		return err
	}
	v.size = &size

	return nil
}

// LsDir returns number of directories in a path
func (v *VFS) LsDir(path string) ([]string, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	folderData := FolderData{
		Folders: []string{},
		Vfs:     v,
	}
	data := pointer.Save(&folderData)

	ret := C._list_of_folders_in_path(v.context.tiledbContext, v.tiledbVFS, cpath, data)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in getting folder list %s: %s", path, v.context.LastError())
	}

	return folderData.Folders, nil
}
