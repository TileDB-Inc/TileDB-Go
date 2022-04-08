//go:build experimental
// +build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"unsafe"
)

/*
   	#cgo LDFLAGS: -ltiledb
   	#cgo linux LDFLAGS: -ldl
	#include <tiledb/tiledb_experimental.h>
	#include <tiledb/tiledb_serialization.h>
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

// Deserialize deserializes the group from the given buffer
func (g *Group) Deserialize(buffer *Buffer, serializationType SerializationType, clientSide bool) error {
	var cClientSide C.int32_t
	if clientSide {
		cClientSide = 1
	} else {
		cClientSide = 0
	}

	b, err := buffer.Data()
	if err != nil {
		return errors.New("failed to retrieve bytes from buffer")
	}

	// cstrings are null terminated. Go's are not, add it as a suffix
	if err := buffer.SetBuffer(append(b, []byte("\u0000")...)); err != nil {
		return errors.New("failed to add null terminator to buffer")
	}

	ret := C.tiledb_deserialize_group(g.context.tiledbContext, buffer.tiledbBuffer, C.tiledb_serialization_type_t(serializationType), cClientSide, g.group)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing group: %s", g.context.LastError())
	}

	return nil
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
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var isSliceValue bool = false
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		isSliceValue = true
	}

	var datatype Datatype
	var valueNum C.uint
	var valueType reflect.Kind

	valueInterfaceVal := reflect.ValueOf(value)
	if isSliceValue {
		if valueInterfaceVal.Len() == 0 {
			return fmt.Errorf("Value passed must be a non-empty slice, size of slice is: %d", valueInterfaceVal.Len())
		}
		valueType = reflect.TypeOf(value).Elem().Kind()
		valueNum = C.uint(valueInterfaceVal.Len())
	} else {
		valueType = reflect.TypeOf(value).Kind()
		valueNum = 1
	}

	var ret C.int32_t
	switch valueType {
	case reflect.Int:
		// Check size of int on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_INT32
			if isSliceValue {
				tmpValue := value.([]int32)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(int32)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		} else {
			datatype = TILEDB_INT64
			if isSliceValue {
				tmpValue := value.([]int64)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(int64)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		}
	case reflect.Int8:
		datatype = TILEDB_INT8
		if isSliceValue {
			tmpValue := value.([]int8)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int8)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int16:
		datatype = TILEDB_INT16
		if isSliceValue {
			tmpValue := value.([]int16)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int16)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int32:
		datatype = TILEDB_INT32
		if isSliceValue {
			tmpValue := value.([]int32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Int64:
		datatype = TILEDB_INT64
		if isSliceValue {
			tmpValue := value.([]int64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(int64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint:
		// Check size of uint on platform
		if strconv.IntSize == 32 {
			datatype = TILEDB_UINT32
			if isSliceValue {
				tmpValue := value.([]uint32)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(uint32)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		} else {
			datatype = TILEDB_UINT64
			if isSliceValue {
				tmpValue := value.([]uint64)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
			} else {
				tmpValue := value.(uint64)
				ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
			}
		}
	case reflect.Uint8:
		datatype = TILEDB_UINT8
		if isSliceValue {
			tmpValue := value.([]uint8)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint8)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint16:
		datatype = TILEDB_UINT16
		if isSliceValue {
			tmpValue := value.([]uint16)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint16)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint32:
		datatype = TILEDB_UINT32
		if isSliceValue {
			tmpValue := value.([]uint32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Uint64:
		datatype = TILEDB_UINT64
		if isSliceValue {
			tmpValue := value.([]uint64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(uint64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Float32:
		datatype = TILEDB_FLOAT32
		if isSliceValue {
			tmpValue := value.([]float32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(float32)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.Float64:
		datatype = TILEDB_FLOAT64
		if isSliceValue {
			tmpValue := value.([]float64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue[0]))
		} else {
			tmpValue := value.(float64)
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&tmpValue))
		}
	case reflect.String:
		datatype = TILEDB_STRING_UTF8
		stringValue := value.(string)
		valueNum = C.uint(len(stringValue))
		cTmpValue := C.CString(stringValue)
		defer C.free(unsafe.Pointer(cTmpValue))
		if valueNum > 0 {
			ret = C.tiledb_group_put_metadata(g.context.tiledbContext, g.group, ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(cTmpValue))
		}
	default:
		if isSliceValue {
			return fmt.Errorf("Unrecognized value type passed: %s", valueInterfaceVal.Index(0).Kind().String())
		}
		return fmt.Errorf("Unrecognized value type passed: %s", valueInterfaceVal.Kind().String())
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding metadata to group: %s", g.context.LastError())
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

func (g *Group) GetMetadata(key string) (Datatype, uint, interface{}, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var cType C.tiledb_datatype_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_group_get_metadata(g.context.tiledbContext, g.group, ckey, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return 0, 0, nil, fmt.Errorf("Error getting metadata from group: %s, key: %s", g.context.LastError(), key)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return 0, 0, nil, fmt.Errorf("Error getting metadata from group, key: %s does not exist", key)
	}

	datatype := Datatype(cType)
	value, err := datatype.GetValue(valueNum, cvalue)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("%s, key: %s", err.Error(), key)
	}

	return datatype, valueNum, value, nil
}

func (g *Group) DeleteMetadata(key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	ret := C.tiledb_group_delete_metadata(g.context.tiledbContext, g.group, ckey)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deleting metadata from group: %s", g.context.LastError())
	}
	return nil
}

func (g *Group) GetMetadataNum() (uint64, error) {
	var cNum C.uint64_t

	ret := C.tiledb_group_get_metadata_num(g.context.tiledbContext, g.group, &cNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting number of metadata from group: %s", g.context.LastError())
	}

	return uint64(cNum), nil
}

func (g *Group) GetMetadataFromIndex(index uint64) (*GroupMetadata, error) {
	return g.GetMetadataFromIndexWithValueLimit(index, nil)
}

func (g *Group) GetMetadataFromIndexWithValueLimit(index uint64, limit *uint) (*GroupMetadata, error) {
	var cKey *C.char

	var cIndex C.uint64_t = C.uint64_t(index)
	var cType C.tiledb_datatype_t
	var cKeyLen C.uint32_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_group_get_metadata_from_index(g.context.tiledbContext,
		g.group, cIndex, &cKey, &cKeyLen, &cType, &cValueNum, &cvalue)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting metadata from group: %s, index: %d", g.context.LastError(), index)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return nil, fmt.Errorf("Error getting metadata from group, Index: %d does not exist", index)
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
		Key:      C.GoString(cKey),
		KeyLen:   uint32(cKeyLen),
		Datatype: datatype,
		ValueNum: valueNum,
		Value:    value,
	}

	return &groupMetadata, nil
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

// SerializeGroupMetadata gets and serializes the group metadata
func SerializeGroupMetadata(g *Group, serializationType SerializationType) (*Buffer, error) {
	buffer := Buffer{context: g.context}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&buffer, func(buffer *Buffer) {
		buffer.Free()
	})

	ret := C.tiledb_serialize_group_metadata(g.context.tiledbContext, g.group, C.tiledb_serialization_type_t(serializationType), &buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error serializing group metadata: %s", g.context.LastError())
	}

	b, err := buffer.Data()
	if err != nil {
		return nil, errors.New("failed to retrieve bytes from buffer")
	}
	// cstrings are null terminated. Go's are not, remove the suffix if it exists
	if err := buffer.SetBuffer(bytes.TrimSuffix(b, []byte("\u0000"))); err != nil {
		return nil, errors.New("failed to remove null terminator from buffer")
	}

	return &buffer, nil
}

// DeserializeGroupMetadata deserializes group metadata
func DeserializeGroupMetadata(g *Group, buffer *Buffer, serializationType SerializationType) error {
	b, err := buffer.Data()
	if err != nil {
		return errors.New("failed to retrieve bytes from buffer")
	}
	// cstrings are null terminated. Go's are not, add it as a suffix
	if err := buffer.SetBuffer(append(b, []byte("\u0000")...)); err != nil {
		return errors.New("failed to add null terminator to buffer")
	}

	ret := C.tiledb_deserialize_group_metadata(g.context.tiledbContext, g.group, C.tiledb_serialization_type_t(serializationType), buffer.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error deserializing group metadata: %s", g.context.LastError())
	}

	return nil
}
