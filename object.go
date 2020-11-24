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
)

// Object implements "object" managemnt in TileDB
// A TileDB "object" is currently either a TileDB array or a TileDB group.
type Object struct {
	context *Context
	path    string
}

func NewObject(context *Context, path string) (*Object, error) {
	if path == "" {
		return nil, errors.New("Object path cnanot be empty")
	}

	object := Object{
		context: context,
		path:    path,
	}

	return &object, nil
}

// Type returns the query type
func (o *Object) Type() (ObjectType, error) {
	var objectType C.tiledb_object_t
	cpath := C.CString(o.path)
	ret := C.tiledb_object_type(o.context.tiledbContext, cpath, &objectType)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Cannot get object type from path %s: %s", o.path, o.context.LastError())
	}
	return ObjectType(objectType), nil
}

// func (Object *o) Type() error {

// }

// func (Object *o) Ls() error {

// }

// func (Object *o) Walk() error {

// }

// func (Object *o) Move() error {

// }

// func (Object *o) Remove() error {

// }
