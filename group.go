package tiledb

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

/*
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

// NewGroup allocates an embedded group.
func NewGroup(tdbCtx *Context, uri string) (*Group, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	group := Group{context: tdbCtx, uri: uri}
	ret := C.tiledb_group_alloc(group.context.tiledbContext, curi, &group.group)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb group: %w", group.context.LastError())
	}
	freeOnGC(&group)

	return &group, nil
}

// Create creates a new TileDB group given a context and URI.
func CreateGroup(tdbCtx *Context, uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_group_create(tdbCtx.tiledbContext, curi)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in creating group: %w", tdbCtx.LastError())
	}
	return nil
}

// Create creates a new TileDB group.
func (g *Group) Create() error {
	return CreateGroup(g.context, g.uri)
}

func (g *Group) Open(queryType QueryType) error {
	ret := C.tiledb_group_open(g.context.tiledbContext, g.group, C.tiledb_query_type_t(queryType))
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error opening tiledb group for querying: %w", g.context.LastError())
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
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error closing tiledb group: %w", g.context.LastError())
	}
	return nil
}

func (g *Group) SetConfig(config *Config) error {
	ret := C.tiledb_group_set_config(g.context.tiledbContext, g.group, config.tiledbConfig)
	runtime.KeepAlive(g)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting config on group: %w", g.context.LastError())
	}
	g.config = config
	return nil
}

func (g *Group) Config() (*Config, error) {
	var config Config
	ret := C.tiledb_group_get_config(g.context.tiledbContext, g.group, &config.tiledbConfig)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting config from query: %w", g.context.LastError())
	}
	freeOnGC(&config)

	if g.config == nil {
		g.config = &config
	}

	return &config, nil
}

func (g *Group) AddMember(uri, name string, isRelativeURI bool) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var cRelative C.uint8_t
	if isRelativeURI {
		cRelative = 1
	}

	ret := C.tiledb_group_add_member(g.context.tiledbContext, g.group, curi, cRelative, cname)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding member to group: %w", g.context.LastError())
	}
	return nil
}

// GroupMetadata defines metadata for the group
type GroupMetadata struct {
	Key      string
	KeyLen   uint32
	Datatype Datatype
	ValueNum uint
	Value    interface{}
}

// MarshalJSON implements the Marshaller interface for GroupMetadata
func (g GroupMetadata) MarshalJSON() ([]byte, error) {
	switch v := g.Value.(type) {
	case []byte:
		return json.Marshal(string(v))
	default:
		return json.Marshal(v)
	}
}

// PutMetadata puts a metadata key-value item to an open group. The group must
// be opened in WRITE mode, otherwise the function will error out.
func (g *Group) PutMetadata(key string, value interface{}) error {
	switch value := value.(type) {
	case int:
		return groupPutScalarMetadata(g, tileDBInt, key, value)
	case []int:
		return groupPutSliceMetadata(g, tileDBInt, key, value)
	case int8:
		return groupPutScalarMetadata(g, TILEDB_INT8, key, value)
	case []int8:
		return groupPutSliceMetadata(g, TILEDB_INT8, key, value)
	case int16:
		return groupPutScalarMetadata(g, TILEDB_INT16, key, value)
	case []int16:
		return groupPutSliceMetadata(g, TILEDB_INT16, key, value)
	case int32:
		return groupPutScalarMetadata(g, TILEDB_INT32, key, value)
	case []int32:
		return groupPutSliceMetadata(g, TILEDB_INT32, key, value)
	case uint:
		return groupPutScalarMetadata(g, tileDBUint, key, value)
	case []uint:
		return groupPutSliceMetadata(g, tileDBUint, key, value)
	case int64:
		return groupPutScalarMetadata(g, TILEDB_INT64, key, value)
	case []int64:
		return groupPutSliceMetadata(g, TILEDB_INT64, key, value)
	case uint8:
		return groupPutScalarMetadata(g, TILEDB_UINT8, key, value)
	case []uint8:
		return groupPutSliceMetadata(g, TILEDB_UINT8, key, value)
	case uint16:
		return groupPutScalarMetadata(g, TILEDB_UINT16, key, value)
	case []uint16:
		return groupPutSliceMetadata(g, TILEDB_UINT16, key, value)
	case uint32:
		return groupPutScalarMetadata(g, TILEDB_UINT32, key, value)
	case []uint32:
		return groupPutSliceMetadata(g, TILEDB_UINT32, key, value)
	case uint64:
		return groupPutScalarMetadata(g, TILEDB_UINT64, key, value)
	case []uint64:
		return groupPutSliceMetadata(g, TILEDB_UINT64, key, value)
	case float32:
		return groupPutScalarMetadata(g, TILEDB_FLOAT32, key, value)
	case []float32:
		return groupPutSliceMetadata(g, TILEDB_FLOAT32, key, value)
	case float64:
		return groupPutScalarMetadata(g, TILEDB_FLOAT64, key, value)
	case []float64:
		return groupPutSliceMetadata(g, TILEDB_FLOAT64, key, value)
	case bool:
		return groupPutScalarMetadata(g, TILEDB_BOOL, key, value)
	case []bool:
		return groupPutSliceMetadata(g, TILEDB_BOOL, key, value)
	case string:
		valPtr := unsafe.Pointer(C.CString(value))
		defer C.free(valPtr)
		return groupPutMetadata(g, TILEDB_STRING_UTF8, key, valPtr, len(value))
	}
	return fmt.Errorf("can't write %q metadata: unrecognized value type %T", key, value)
}

func groupPutSliceMetadata[T scalarType](g *Group, dt Datatype, key string, value []T) error {
	if len(value) == 0 {
		return fmt.Errorf("length of %q metadata %T value must be nonzero", key, value)
	}
	return groupPutMetadata(g, dt, key, slicePtr(value), len(value))
}

func groupPutScalarMetadata[T scalarType](g *Group, dt Datatype, key string, value T) error {
	return groupPutMetadata(g, dt, key, unsafe.Pointer(&value), 1)
}

func groupPutMetadata(g *Group, dt Datatype, key string, valuePtr unsafe.Pointer, count int) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	ret := C.tiledb_group_put_metadata(
		g.context.tiledbContext,
		g.group,
		cKey,
		C.tiledb_datatype_t(dt),
		C.uint(count),
		valuePtr,
	)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("could not add metadata to group: %w", g.context.LastError())
	}
	return nil
}

func (g *Group) RemoveMember(uri string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_group_remove_member(g.context.tiledbContext, g.group, curi)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error removing member from group: %w", g.context.LastError())
	}
	return nil
}

func (g *Group) GetMemberCount() (uint64, error) {
	var count C.uint64_t
	ret := C.tiledb_group_get_member_count(g.context.tiledbContext, g.group, &count)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error retrieving member count in group: %w", g.context.LastError())
	}
	return uint64(count), nil
}

func (g *Group) GetMemberFromIndex(index uint64) (string, string, ObjectTypeEnum, error) {
	var curi *C.tiledb_string_t

	var cname *C.tiledb_string_t

	var objectTypeEnum C.tiledb_object_t
	ret := C.tiledb_group_get_member_by_index_v2(g.context.tiledbContext, g.group, C.uint64_t(index), &curi, &objectTypeEnum, &cname)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return "", "", TILEDB_INVALID, fmt.Errorf("error getting member by index for group: %w", g.context.LastError())
	}
	defer C.tiledb_string_free(&curi)
	defer C.tiledb_string_free(&cname)

	uri, err := stringHandleToString(curi)
	if err != nil {
		return "", "", TILEDB_INVALID, err
	}

	name, err := stringHandleToString(cname)
	if err != nil {
		return "", "", TILEDB_INVALID, err
	}

	return uri, name, ObjectTypeEnum(objectTypeEnum), nil
}

func (g *Group) GetMemberByName(name string) (string, string, ObjectTypeEnum, error) {
	var curi *C.tiledb_string_t

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var objectTypeEnum C.tiledb_object_t
	ret := C.tiledb_group_get_member_by_name_v2(g.context.tiledbContext, g.group, cname, &curi, &objectTypeEnum)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return "", "", TILEDB_INVALID, fmt.Errorf("error getting member by index for group: %w", g.context.LastError())
	}
	defer C.tiledb_string_free(&curi)

	uri, err := stringHandleToString(curi)
	if err != nil {
		return "", "", TILEDB_INVALID, err
	}

	if name == "" {
		return "", "", TILEDB_INVALID, fmt.Errorf("error getting name for member %s: name is empty", name)
	}

	return uri, name, ObjectTypeEnum(objectTypeEnum), nil
}

func (g *Group) GetMetadata(key string) (Datatype, uint, interface{}, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var cType C.tiledb_datatype_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer // g must be kept alive while cvalue is being accessed.

	ret := C.tiledb_group_get_metadata(g.context.tiledbContext, g.group, ckey, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return 0, 0, nil, fmt.Errorf("error getting metadata from group: %w, key: %s", g.context.LastError(), key)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return 0, 0, nil, fmt.Errorf("error getting metadata from group, key: %s does not exist", key)
	}

	datatype := Datatype(cType)
	value, err := datatype.GetValue(valueNum, cvalue)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("%w, key: %s", err, key)
	}

	runtime.KeepAlive(g)
	return datatype, valueNum, value, nil
}

func (g *Group) DeleteMetadata(key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	ret := C.tiledb_group_delete_metadata(g.context.tiledbContext, g.group, ckey)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deleting metadata from group: %w", g.context.LastError())
	}
	return nil
}

func (g *Group) GetMetadataNum() (uint64, error) {
	var cNum C.uint64_t

	ret := C.tiledb_group_get_metadata_num(g.context.tiledbContext, g.group, &cNum)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting number of metadata from group: %w", g.context.LastError())
	}

	return uint64(cNum), nil
}

func (g *Group) GetMetadataFromIndex(index uint64) (*GroupMetadata, error) {
	return g.GetMetadataFromIndexWithValueLimit(index, nil)
}

func (g *Group) GetMetadataFromIndexWithValueLimit(index uint64, limit *uint) (*GroupMetadata, error) {
	var cKey *C.char // g must be kept alive while cKey is being accessed.

	var cIndex C.uint64_t = C.uint64_t(index)
	var cType C.tiledb_datatype_t
	var cKeyLen C.uint32_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer // g must be kept alive while cvalue is being accessed.

	ret := C.tiledb_group_get_metadata_from_index(g.context.tiledbContext,
		g.group, cIndex, &cKey, &cKeyLen, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting metadata from group: %s, index: %d", g.context.LastError(), index)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return nil, fmt.Errorf("error getting metadata from group, Index: %d does not exist", index)
	}

	datatype := Datatype(cType)
	if limit != nil && valueNum > *limit {
		valueNum = *limit
	}
	value, err := datatype.GetValue(valueNum, cvalue)
	if err != nil {
		return nil, fmt.Errorf("%s, Index: %d", err.Error(), index)
	}

	groupMetadata := GroupMetadata{
		Key:      C.GoStringN(cKey, C.int(cKeyLen)),
		KeyLen:   uint32(cKeyLen),
		Datatype: datatype,
		ValueNum: valueNum,
		Value:    value,
	}

	runtime.KeepAlive(g)
	return &groupMetadata, nil
}

// Dump the Group to a string value
func (g *Group) Dump(recurse bool) (string, error) {
	queryType, err := g.QueryType()
	if err != nil {
		return "", fmt.Errorf("error dumping group to string: %w", err)
	} else if queryType != TILEDB_READ {
		return "", errors.New("error dumping group to string: group must be opened in TILEDB_READ mode")
	}

	var tdbString *C.tiledb_string_t

	var cRecurse C.uint8_t
	if recurse {
		cRecurse = 1
	}

	ret := C.tiledb_group_dump_str_v2(g.context.tiledbContext, g.group, &tdbString, cRecurse)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error dumping group contents: %w", g.context.LastError())
	}
	defer C.tiledb_string_free(&tdbString)

	dumpStr, err := stringHandleToString(tdbString)
	if err != nil {
		return "", fmt.Errorf("error dumping group contents: %w", g.context.LastError())
	}

	return dumpStr, nil
}

// GetIsRelativeURIByName returns whether a named member of the group has a uri relative to the group
func (g *Group) GetIsRelativeURIByName(name string) (bool, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var isRelative C.uint8_t
	ret := C.tiledb_group_get_is_relative_uri_by_name(g.context.tiledbContext, g.group, cName, &isRelative)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error getting if member %s has a relative uri: %w", name, g.context.LastError())
	}
	return isRelative > 0, nil
}

// Delete deletes written data from an open group. The group must be opened in MODIFY_EXCLUSIVE mode,
// otherwise the function will error out.
// Set recursive true if all data inside the group is to be deleted.
func (g *Group) Delete(recursive bool) error {
	curi := C.CString(g.uri)
	defer C.free(unsafe.Pointer(curi))

	var cRecursive C.uint8_t
	if recursive {
		cRecursive = 1
	}

	ret := C.tiledb_group_delete_group(g.context.tiledbContext, g.group, curi, cRecursive)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deleting group: %w", g.context.LastError())
	}
	return nil
}

// AddMemberWithType adds a member to the Group providing its type.
// This method is recommended for performance when operating on remote groups.
func (g *Group) AddMemberWithType(uri, name string, isRelativeURI bool, objectType ObjectTypeEnum) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	var cRelative C.uint8_t
	if isRelativeURI {
		cRelative = 1
	}

	ret := C.tiledb_group_add_member_with_type(g.context.tiledbContext, g.group, curi, cRelative, cname, C.tiledb_object_t(objectType))
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding member with type to group: %w", g.context.LastError())
	}
	return nil
}

// IsOpen returns true if the Group is open or false if the group is closed.
func (g *Group) IsOpen() (bool, error) {
	var isOpen C.int32_t

	ret := C.tiledb_group_is_open(g.context.tiledbContext, g.group, &isOpen)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error checking if group is open: %w", g.context.LastError())
	}

	return isOpen > 0, nil
}

// QueryType returns the QueryType for the currently opened group.
func (g *Group) QueryType() (QueryType, error) {
	var queryType C.tiledb_query_type_t

	ret := C.tiledb_group_get_query_type(g.context.tiledbContext, g.group, &queryType)
	runtime.KeepAlive(g)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error retrieving group QueryType: %w", g.context.LastError())
	}

	return QueryType(queryType), nil
}
