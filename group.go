package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// GroupCreate creates a new tiledb group. A Group is a logical grouping
// of Objects on the storage system (a directory).
func GroupCreate(context *Context, group string) error {
	cgroup := C.CString(group)
	defer C.free(unsafe.Pointer(cgroup))

	ret := C.tiledb_group_create(context.tiledbContext, cgroup)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in creating group %s: %s", group, context.LastError())
	}
	return nil
}
