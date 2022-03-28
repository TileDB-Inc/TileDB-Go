//go:build experimental
// +build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

import (
	"fmt"
	"runtime"
	"unsafe"
)

/*
   	#cgo LDFLAGS: -ltiledb
   	#cgo linux LDFLAGS: -ldl
	#include <tiledb/tiledb_experimental.h>
	#include <stdlib.h>
*/
import "C"

// Group represents a wrapped TileDB embedded group
type Group struct {
	group   *C.tiledb_group_t
	uri     string
	context *Context
	config  *Config
}

// NewGroup allocates an embedded group
func NewGroup(tdbCtx *Context, uri string) (*Group, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	group := Group{context: tdbCtx, uri: uri}
	ret := C.tiledb_group_alloc(group.context.tiledbContext, curi, &group.group)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb group: %s", group.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&group, func(group *Group) {
		group.Free()
	})

	return &group, nil
}

// Create a new TileDB group
func (g *Group) Create() error {
	curi := C.CString(g.uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_group_create(g.context.tiledbContext, curi)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in creating group: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) Open(queryType QueryType) error {
	ret := C.tiledb_group_open(g.context.tiledbContext, g.group, C.tiledb_query_type_t(queryType))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error opening tiledb group for querying: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) Free() {
	if g.group != nil {
		g.Close()
		C.tiledb_group_free(&g.group)
	}
}

func (g *Group) Close() error {
	ret := C.tiledb_group_close(g.context.tiledbContext, g.group)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error closing tiledb group: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) SetConfig(config *Config) error {
	ret := C.tiledb_group_set_config(g.context.tiledbContext, g.group, config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting config on group: %s", g.context.LastError())
	}
	g.config = config
	return nil
}

func (g *Group) Config() (*Config, error) {
	var config Config
	ret := C.tiledb_group_get_config(g.context.tiledbContext, g.group, &config.tiledbConfig)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting config from query: %s", g.context.LastError())
	}

	runtime.SetFinalizer(&config, func(config *Config) {
		config.Free()
	})

	if g.config == nil {
		g.config = &config
	}

	return &config, nil
}

func (g *Group) AddMember(uri string, isRelativeURI bool) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var cRelative C.uint8_t
	if isRelativeURI {
		cRelative = 1
	}

	ret := C.tiledb_group_add_member(g.context.tiledbContext, g.group, curi, cRelative)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding member to group: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) RemoveMember(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_group_remove_member(g.context.tiledbContext, g.group, curi)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error removing member from group: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) GetMemberCount() (uint64, error) {
	var count C.uint64_t
	ret := C.tiledb_group_get_member_count(g.context.tiledbContext, g.group, &count)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error retrieving member count in group: %s", g.context.LastError())
	}
	return uint64(count), nil
}

func (g *Group) GetMemberFromIndex(index uint64) (string, ObjectTypeEnum, error) {
	var curi *C.char
	defer C.free(unsafe.Pointer(curi))

	var objectTypeEnum C.tiledb_object_t
	ret := C.tiledb_group_get_member_by_index(g.context.tiledbContext, g.group, C.uint64_t(index), &curi, &objectTypeEnum)
	if ret != C.TILEDB_OK {
		return "", TILEDB_INVALID, fmt.Errorf("Error getting member by index for group: %s", g.context.LastError())
	}

	uri := C.GoString(curi)
	if uri == "" {
		return "", TILEDB_INVALID, fmt.Errorf("Error getting URI for member %d: uri is empty", index)
	}

	return uri, ObjectTypeEnum(objectTypeEnum), nil
}

func (g *Group) Dump(recurse bool) (string, error) {
	var cOutput *C.char
	defer C.free(unsafe.Pointer(cOutput))

	var cRecurse C.uint8_t
	if recurse {
		cRecurse = 1
	}

	ret := C.tiledb_group_dump_str(g.context.tiledbContext, g.group, &cOutput, cRecurse)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error dumping group contents: %s", g.context.LastError())
	}

	return C.GoString(cOutput), nil
}
