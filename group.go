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

// GroupCreate creates a new tiledb group
func GroupCreate(context *Context, group string) error {
	cgroup := C.CString(group)
	defer C.free(unsafe.Pointer(cgroup))

	ret := C.tiledb_group_create(context.tiledbContext, cgroup)
	if ret == C.TILEDB_ERR {
		return fmt.Errorf("Error in creating group %s: %s", group, context.GetLastError())
	}
	return nil
}
