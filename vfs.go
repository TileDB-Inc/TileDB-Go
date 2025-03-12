package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
#include "clibrary.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"unsafe"

	pointer "github.com/mattn/go-pointer"
)

const arrayMetadataFolderName = "__meta"

type vfsFhHandle struct{ *capiHandle }

func freeCapiVfsFh(c unsafe.Pointer) {
	C.tiledb_vfs_fh_free((**C.tiledb_vfs_fh_t)(unsafe.Pointer(&c)))
}

func newVfsFhHandle(ptr *C.tiledb_vfs_fh_t) vfsFhHandle {
	return vfsFhHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiVfsFh)}
}

func (x vfsFhHandle) Get() *C.tiledb_vfs_fh_t {
	return (*C.tiledb_vfs_fh_t)(x.capiHandle.Get())
}

// VFSfh is a virtual file system file handler
type VFSfh struct {
	vfs         *VFS
	tiledbVFSfh vfsFhHandle
	context     *Context
	offset      uint64
	size        *uint64
	uri         string
}

func newVfsFhFromHandle(context *Context, vfs *VFS, uri string, handle vfsFhHandle) *VFSfh {
	return &VFSfh{vfs: vfs, tiledbVFSfh: handle, context: context, uri: uri}
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (v *VFSfh) Free() {
	v.tiledbVFSfh.Free()
}

// Context exposes the internal TileDB context used to initialize the vfsh.
func (v *VFSfh) Context() *Context {
	return v.context
}

// IsClosed checks a vfs file handler to see if it is closed. Return true if
// file handler is closed, false if its not closed and error is non-nil on error
func (v *VFSfh) IsClosed() (bool, error) {
	var isClosed C.int32_t

	ret := C.tiledb_vfs_fh_is_closed(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get(), &isClosed)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return false, errors.New("error in checking if vfs file handler is closed")
	}

	if isClosed == 1 {
		return true, nil
	}

	return false, nil
}

type vfsHandle struct{ *capiHandle }

func freeCapiVfs(c unsafe.Pointer) {
	C.tiledb_vfs_free((**C.tiledb_vfs_t)(unsafe.Pointer(&c)))
}

func newVfsHandle(ptr *C.tiledb_vfs_t) vfsHandle {
	return vfsHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiVfs)}
}

func (x vfsHandle) Get() *C.tiledb_vfs_t {
	return (*C.tiledb_vfs_t)(x.capiHandle.Get())
}

// VFS Implements a virtual filesystem that enables performing directory/file
// operations with a unified API on different filesystems, such as local
// posix/windows, HDFS, AWS S3, etc.
type VFS struct {
	tiledbVFS vfsHandle
	context   *Context
}

func newVfsFromHandle(context *Context, handle vfsHandle) *VFS {
	return &VFS{context: context, tiledbVFS: handle}
}

// NewVFS alloc a new context using tiledb_vfs_alloc. This also registers the
// `runtime.SetFinalizer` for handling the free'ing of the c data structure on
// garbage collection
func NewVFS(context *Context, config *Config) (*VFS, error) {
	var vfsPtr *C.tiledb_vfs_t
	ret := C.tiledb_vfs_alloc(context.tiledbContext.Get(), config.tiledbConfig.Get(), &vfsPtr)
	runtime.KeepAlive(context)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb VFS: %w", context.LastError())
	}

	return newVfsFromHandle(context, newVfsHandle(vfsPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (v *VFS) Free() {
	v.tiledbVFS.Free()
}

// Context exposes the internal TileDB context used to initialize the vfs.
func (v *VFS) Context() *Context {
	return v.context
}

// Config retrieves a copy of the config from vfs.
func (v *VFS) Config() (*Config, error) {
	var configPtr *C.tiledb_config_t
	ret := C.tiledb_vfs_get_config(v.context.tiledbContext.Get(), v.tiledbVFS.Get(),
		&configPtr)

	if ret == C.TILEDB_OOM {
		return nil, errors.New("out of Memory error in GetConfig")
	} else if ret != C.TILEDB_OK {
		return nil, errors.New("unknown error in GetConfig")
	}

	return newConfigFromHandle(newConfigHandle(configPtr)), nil
}

// CreateBucket creates an object-store bucket with the input URI.
func (v *VFS) CreateBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_create_bucket(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in creating s3 bucket %s: %w", uri, v.context.LastError())
	}

	return nil
}

// RemoveBucket deletes an object-store bucket with the input URI.
func (v *VFS) RemoveBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_remove_bucket(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in removing s3 bucket %s: %w", uri, v.context.LastError())
	}

	return nil
}

// EmptyBucket empties a bucket.
func (v *VFS) EmptyBucket(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_empty_bucket(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in emptying s3 bucket %s: %w", uri, v.context.LastError())
	}

	return nil
}

// IsEmptyBucket checks if a bucket is empty.
func (v *VFS) IsEmptyBucket(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isEmpty C.int32_t
	ret := C.tiledb_vfs_is_empty_bucket(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &isEmpty)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if s3 bucket %s is empty: %w", uri, v.context.LastError())
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
	ret := C.tiledb_vfs_is_bucket(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &isBucket)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if %s is a s3 bucket: %w", uri, v.context.LastError())
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
	ret := C.tiledb_vfs_create_dir(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in creating directory %s: %w", uri, v.context.LastError())
	}

	return nil
}

// IsDir checks if a directory with the input URI exists.
func (v *VFS) IsDir(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isDir C.int32_t
	ret := C.tiledb_vfs_is_dir(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &isDir)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if %s is a directory: %w", uri, v.context.LastError())
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
	ret := C.tiledb_vfs_remove_dir(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in removing directory %s: %w", uri, v.context.LastError())
	}

	return nil
}

// IsFile checks if a file with the input URI exists.
func (v *VFS) IsFile(uri string) (bool, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var isFile C.int32_t
	ret := C.tiledb_vfs_is_file(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &isFile)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error in checking if %s is a file: %w", uri, v.context.LastError())
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
	ret := C.tiledb_vfs_remove_file(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in removing file %s: %w", uri, v.context.LastError())
	}

	return nil
}

// FileSize retrieves the size of a file.
func (v *VFS) FileSize(uri string) (uint64, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var cfsize C.uint64_t
	ret := C.tiledb_vfs_file_size(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &cfsize)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error in getting file size %s: %w", uri, v.context.LastError())
	}

	return uint64(cfsize), nil
}

// MoveFile renames a TileDB file from an old URI to a new URI.
func (v *VFS) MoveFile(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_move_file(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cOldURI, cNewURI)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in moving file %s to %s: %w", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// CopyFile renames a TileDB file from an old URI to a new URI.
func (v *VFS) CopyFile(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_copy_file(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cOldURI, cNewURI)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in copying file %s to %s: %w", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// MoveDir menames a TileDB directory from an old URI to a new URI.
func (v *VFS) MoveDir(oldURI string, newURI string) error {
	cOldURI := C.CString(oldURI)
	defer C.free(unsafe.Pointer(cOldURI))
	cNewURI := C.CString(newURI)
	defer C.free(unsafe.Pointer(cNewURI))

	ret := C.tiledb_vfs_move_dir(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cOldURI, cNewURI)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in moving directory %s to %s: %w", oldURI, newURI, v.context.LastError())
	}

	return nil
}

// Open prepares a file for reading/writing.
func (v *VFS) Open(uri string, mode VFSMode) (*VFSfh, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	var fhPtr *C.tiledb_vfs_fh_t
	ret := C.tiledb_vfs_open(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, C.tiledb_vfs_mode_t(mode), &fhPtr)
	runtime.KeepAlive(v)

	if ret == C.TILEDB_OOM {
		return nil, fmt.Errorf("out of Memory error in VFS.Open: %w", v.context.LastError())
	} else if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("unknown error in VFS.Open: %w", v.context.LastError())
	}

	return newVfsFhFromHandle(v.context, v, uri, newVfsFhHandle(fhPtr)), nil
}

// Close closes a file. This is flushes the buffered data into the file when the file
// was opened in write (or append) mode. It is particularly important to be
// called after S3 writes, as otherwise the writes will not take effect.
func (v *VFS) Close(fh *VFSfh) error {
	ret := C.tiledb_vfs_close(v.context.tiledbContext.Get(), fh.tiledbVFSfh.Get())
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Close: %w", v.context.LastError())
	}

	fh.Free()
	return nil
}

// Read reads part of a file.
func (v *VFS) Read(fh *VFSfh, offset uint64, nbytes uint64) ([]byte, error) {
	bytes := make([]byte, nbytes)
	cbuffer := slicePtr(bytes)
	ret := C.tiledb_vfs_read(v.context.tiledbContext.Get(), fh.tiledbVFSfh.Get(), C.uint64_t(offset), cbuffer, C.uint64_t(nbytes))
	runtime.KeepAlive(v)
	runtime.KeepAlive(fh)

	if ret != C.TILEDB_OK {
		return []byte{}, fmt.Errorf("unknown error in VFS.Read: %w", v.context.LastError())
	}

	return bytes, nil
}

// Write writes the contents of a buffer into a file. Note that this function only
// appends data at the end of the file. If the file does not exist,
// it will be created.
func (v *VFS) Write(fh *VFSfh, bytes []byte) error {
	cbuffer := slicePtr(bytes)
	defer runtime.KeepAlive(bytes)
	ret := C.tiledb_vfs_write(v.context.tiledbContext.Get(), fh.tiledbVFSfh.Get(), cbuffer, C.uint64_t(len(bytes)))
	runtime.KeepAlive(v)
	runtime.KeepAlive(fh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Write: %w", v.context.LastError())
	}

	return nil
}

// Sync flushes a file.
func (v *VFS) Sync(fh *VFSfh) error {
	ret := C.tiledb_vfs_sync(v.context.tiledbContext.Get(), fh.tiledbVFSfh.Get())
	runtime.KeepAlive(v)
	runtime.KeepAlive(fh)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Sync: %w", v.context.LastError())
	}

	return nil
}

// Touch touches a file, i.e., creates a new empty file.
func (v *VFS) Touch(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_vfs_touch(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in touching %s: %w", uri, v.context.LastError())
	}

	return nil
}

// DirSize retrieves the size of a directory.
func (v *VFS) DirSize(uri string) (uint64, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var cfsize C.uint64_t
	ret := C.tiledb_vfs_dir_size(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), curi, &cfsize)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error in getting dir size %s: %w", uri, v.context.LastError())
	}

	return uint64(cfsize), nil
}

// NumOfFragmentsData is a type
type NumOfFragmentsData struct {
	NumOfFolders int
	Vfs          *VFS
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

// NumOfFragmentsInPath returns the number of folders in a path.
func (v *VFS) NumOfFragmentsInPath(path string) (int, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	numOfFragmentsData := NumOfFragmentsData{
		NumOfFolders: 0,
		Vfs:          v,
	}
	data := pointer.Save(&numOfFragmentsData)
	defer pointer.Unref(data)

	ret := C._num_of_folders_in_path(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cpath, data)
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error in getting dir list %s: %w", path, v.context.LastError())
	}

	return numOfFragmentsData.NumOfFolders, nil
}

// Close closes a file. This flushes the buffered data into the file when the file
// was opened in write (or append) mode. It is particularly important to be
// called after S3 writes, as otherwise the writes will not take effect.
func (v *VFSfh) Close() error {

	ret := C.tiledb_vfs_close(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get())
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Close: %w", v.context.LastError())
	}

	v.Free()
	return nil
}

// Read reads part of a file.
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

	cbuffer := slicePtr(p)
	ret := C.tiledb_vfs_read(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get(), C.uint64_t(v.offset), cbuffer, C.uint64_t(nbytes))
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("unknown error in VFS.Read: %w", v.context.LastError())
	}

	v.offset += nbytes
	return int(nbytes), nil
}

// ReadAt reads part of a file at a given offset, without updating the object's internal offset.
func (v *VFSfh) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("offset cannot be negative")
	}

	nbytes := uint64(len(p))

	// If the size is empty, fetch it
	if v.size == nil {
		err := v.fetchAndSetSize()
		if err != nil {
			return 0, err
		}
	}

	// If the requested read size is beyond the limit, truncate the read size.
	// In this case we need to return io.EOF.
	var err error = nil
	if uint64(off)+nbytes >= *v.size {
		if uint64(off) > *v.size {
			return 0, io.EOF
		}
		nbytes = *v.size - uint64(off)
		err = io.EOF
	}

	if nbytes == 0 {
		return 0, err
	}

	cbuffer := slicePtr(p)
	ret := C.tiledb_vfs_read(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get(), C.uint64_t(off), cbuffer, C.uint64_t(nbytes))
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("unknown error in VFS.Read: %w", v.context.LastError())
	}

	return int(nbytes), err
}

// Write writes the contents of a buffer into a file. Note that this function only
// appends data at the end of the file. If the file does not exist,
// it will be created.
func (v *VFSfh) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}
	cbuffer := slicePtr(bytes)
	ret := C.tiledb_vfs_write(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get(), cbuffer, C.uint64_t(len(bytes)))
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("unknown error in VFS.Write: %w", v.context.LastError())
	}

	return len(bytes), nil
}

// Sync flushes a file.
func (v *VFSfh) Sync() error {
	ret := C.tiledb_vfs_sync(v.context.tiledbContext.Get(), v.tiledbVFSfh.Get())
	runtime.KeepAlive(v)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("unknown error in VFS.Sync: %w", v.context.LastError())
	}

	return nil
}

// Seek seeks to an offset.
func (v *VFSfh) Seek(offset int64, whence int) (int64, error) {
	if v.size == nil {
		if err := v.fetchAndSetSize(); err != nil {
			return -1, err
		}
	}

	var origin uint64
	switch whence {
	case io.SeekStart:
		origin = 0
	case io.SeekCurrent:
		origin = v.offset
	case io.SeekEnd:
		origin = *v.size
	default:
		return -1, errors.New("unknown seek whence")
	}

	var newOffset uint64
	if offset >= 0 {
		newOffset = origin + uint64(offset)
		if newOffset > *v.size {
			return -1, errors.New("invalid offset, attempt to move beyond end of file")
		}
	} else {
		if offset == math.MinInt64 || uint64(-offset) > origin {
			return -1, errors.New("invalid offset, attempt to move before start of file")
		}
		newOffset = origin - uint64(-offset)
	}

	v.offset = newOffset
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

// FolderData is a type encapsulating list of folders and files
type FolderData struct {
	Folders []string
	Files   []string
	Vfs     *VFS
}

//export vfsLs
func vfsLs(path *C.cchar_t, data unsafe.Pointer) int32 {
	folderData := pointer.Restore(data).(*FolderData)

	uri := C.GoString(path)

	isDir, err := folderData.Vfs.IsDir(uri)
	if err != nil {
		return 0
	}

	if isDir {
		folderData.Folders = append(folderData.Folders, uri)
	} else {
		folderData.Files = append(folderData.Files, uri)
	}

	return 1
}

// List returns list of folders and files in a path.
func (v *VFS) List(path string) ([]string, []string, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	folderData := FolderData{
		Folders: []string{},
		Files:   []string{},
		Vfs:     v,
	}
	data := pointer.Save(&folderData)
	defer pointer.Unref(data)

	ret := C._vfs_ls(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cpath, data)
	runtime.KeepAlive(v)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error in getting path listing %s: %w", path, v.context.LastError())
	}

	return folderData.Folders, folderData.Files, nil
}

// VisitRecursiveCallback gets called by VFS.VisitRecursive. It returns whether visiting should
// continue, and maybe an error to propagate to the caller. If err is not nil, visiting always
// stops.
type VisitRecursiveCallback = func(path string, size uint64) (doContinue bool, err error)

// visitRecursiveState contains the state of a call to VisitRecursive.
type visitRecursiveState struct {
	callback  VisitRecursiveCallback
	lastError error
}

//export vfsLsRecursive
func vfsLsRecursive(path *C.cchar_t, path_len C.size_t, size C.uint64_t, data unsafe.Pointer) int32 {
	state := pointer.Restore(data).(*visitRecursiveState)

	if path_len > math.MaxInt {
		state.lastError = errors.New("path is too long")
		return 0
	}

	doContinue, err := state.callback(C.GoStringN(path, C.int(path_len)), uint64(size))

	if err != nil || !doContinue {
		// Save error to return to the user.
		state.lastError = err
		return 0
	}

	return 1
}

// VisitRecursive calls a function for every file in a path recursively.
// This function returns if the listing ends, or if the callback returns false or an error.
func (v *VFS) VisitRecursive(path string, callback VisitRecursiveCallback) error {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	state := &visitRecursiveState{
		callback:  callback,
		lastError: nil,
	}
	data := pointer.Save(state)
	defer pointer.Unref(data)

	ret := C._vfs_ls_recursive(v.context.tiledbContext.Get(), v.tiledbVFS.Get(), cpath, data)
	runtime.KeepAlive(v)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in recursively listing path %s: %w", path, v.context.LastError())
	}

	return state.lastError
}
