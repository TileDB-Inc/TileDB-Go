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
	"errors"
	"fmt"
	"unsafe"
)

// Object implements "object" managemnt in TileDB
// A TileDB "object" is currently either a TileDB array or a TileDB group.
type Object struct {
	context *Context
	path    string
}

func NewObject(context *Context, path string) (*Object, error) {
	if path == "" {
		return nil, errors.New("Object path cannot be empty")
	}
	object := Object{
		context: context,
		path:    path,
	}
	return &object, nil
}

// Type returns the object type
func (o *Object) Type() (ObjectType, error) {
	var objectType C.tiledb_object_t
	cpath := C.CString(o.path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.tiledb_object_type(o.context.tiledbContext, cpath, &objectType)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Cannot get object type from path %s: %s",
			o.path, o.context.LastError())
	}
	return ObjectType(objectType), nil
}

// Walk (iterates) over the TileDB objects contained in *path*. The traversal
// is done recursively in the order defined by the user. The user provides
// a callback function which is applied on each of the visited TileDB objects.
// The iteration continues for as long the callback returns non-zero, and stops
// when the callback returns 0. Note that this function ignores any object
// (e.g., file or directory) that is not TileDB-related.
func (o *Object) Walk() error {
	return nil
}

// Ls is similar to `tiledb_walk`, but now the function visits only the children
// of `path` (it does not recursively continue to the children directories).
func (o *Object) Ls() error {
	return nil
}

// Move moves a TileDB resource (group, array, key-value).
// Param path is the new path to move to
func (o *Object) Move(newPath string) error {
	cpath := C.CString(o.path)
	defer C.free(unsafe.Pointer(cpath))
	cnewPath := C.CString(newPath)
	defer C.free(unsafe.Pointer(cnewPath))
	ret := C.tiledb_object_move(o.context.tiledbContext, cpath, cnewPath)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Cannot move object from %s to %s: %s", o.path,
			newPath, o.context.LastError())
	}
	// Update object path
	o.path = newPath
	return nil
}

// Remove deletes a TileDB resource (group, array, key-value).
func (o *Object) Remove() error {
	cpath := C.CString(o.path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.tiledb_object_remove(o.context.tiledbContext, cpath)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Cannot delete object %s: %s", o.path,
			o.context.LastError())
	}
	return nil
}
