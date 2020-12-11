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
	"unsafe"

	pointer "github.com/mattn/go-pointer"
)

// ObjectType returns the object type
// A TileDB "object" is currently either a TileDB array or a TileDB group.
func ObjectType(ctx *Context, path string) (ObjectTypeEnum, error) {
	if ctx == nil {
		return TILEDB_INVALID, fmt.Errorf("error getting object type, context is nil")
	}

	var objectTypeEnum C.tiledb_object_t
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.tiledb_object_type(ctx.tiledbContext, cpath, &objectTypeEnum)
	if ret != C.TILEDB_OK {
		return TILEDB_INVALID, fmt.Errorf("Cannot get object type from path %s: %s",
			path, ctx.LastError())
	}

	return ObjectTypeEnum(objectTypeEnum), nil
}

type groupDefinition struct {
	objectTypeEnum ObjectTypeEnum
	path           string
}

// ObjectList defines the value of data returned by object iteration callback
type ObjectList struct {
	objectList []groupDefinition
}

//export objectsInPath
func objectsInPath(path *C.cchar_t, objectTypeEnum C.tiledb_object_t, data unsafe.Pointer) int32 {
	objectData := pointer.Restore(data).(*ObjectList)

	groupDefinition := groupDefinition{
		objectTypeEnum: ObjectTypeEnum(objectTypeEnum),
		path:           C.GoString(path),
	}

	objectData.objectList = append(objectData.objectList, groupDefinition)

	return 1
}

// ObjectWalk (iterates) over the TileDB objects contained in *path*. The traversal
// is done recursively in the order defined by the user. The user provides
// a callback function which is applied on each of the visited TileDB objects.
// The iteration continues for as long the callback returns non-zero, and stops
// when the callback returns 0. Note that this function ignores any object
// (e.g., file or directory) that is not TileDB-related.
func ObjectWalk(ctx *Context, path string, walkOrder WalkOrder) (*ObjectList, error) {
	if ctx == nil {
		return nil, fmt.Errorf("error walking object, context is nil")
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	objectList := ObjectList{
		objectList: []groupDefinition{},
	}
	data := pointer.Save(&objectList)

	ret := C._tiledb_object_walk(ctx.tiledbContext, cpath,
		C.tiledb_walk_order_t(walkOrder), unsafe.Pointer(data))

	fmt.Println(objectList)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Cannot walk in path %s: %s", path,
			ctx.LastError())
	}
	return &objectList, nil
}

// ObjectLs is similar to `tiledb_walk`, but now the function visits only the children
// of `path` (it does not recursively continue to the children directories).
func ObjectLs(ctx *Context, path string) (*ObjectList, error) {
	if ctx == nil {
		return nil, fmt.Errorf("error listing object, context is nil")
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	objectList := ObjectList{
		objectList: []groupDefinition{},
	}
	data := pointer.Save(&objectList)

	ret := C._tiledb_object_ls(ctx.tiledbContext, cpath,
		unsafe.Pointer(data))

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Cannot walk in path %s: %s", path,
			ctx.LastError())
	}
	return &objectList, nil
}

// ObjectMove moves a TileDB resource (group, array, key-value).
// Param path is the new path to move to
func ObjectMove(ctx *Context, path string, newPath string) error {
	if ctx == nil {
		return fmt.Errorf("error moving object, context is nil")
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	cnewPath := C.CString(newPath)
	defer C.free(unsafe.Pointer(cnewPath))
	ret := C.tiledb_object_move(ctx.tiledbContext, cpath, cnewPath)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Cannot move object from %s to %s: %s", path,
			newPath, ctx.LastError())
	}

	return nil
}

// ObjectRemove deletes a TileDB resource (group, array, key-value).
func ObjectRemove(ctx *Context, path string) error {
	if ctx == nil {
		return fmt.Errorf("error removing object, context is nil")
	}

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.tiledb_object_remove(ctx.tiledbContext, cpath)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Cannot delete object %s: %s", path, ctx.LastError())
	}
	return nil
}
