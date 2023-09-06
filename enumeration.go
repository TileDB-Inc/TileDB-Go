//go:build experimental

package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"unsafe"
)

// Enumeration is a TileDB enumeration for Attributes
type Enumeration struct {
	context    *Context
	tiledbEnum *C.tiledb_enumeration_t
}

// EnumerationType is a constraint on valid types for Enumerations
type EnumerationType interface {
	~string | ~float32 | ~float64 | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~int8 | ~int16 | ~int32 | ~int64 | ~bool
}

// enumerationTypeToTileDB maps an EnumerationType to a TileDB Datatype
// Conforms to https://github.com/TileDB-Inc/TileDB/blob/dev/tiledb/sm/cpp_api/type.h
func enumerationTypeToTileDB[T EnumerationType]() Datatype {
	switch reflect.TypeOf((*T)(nil)).Elem().Kind() {
	case reflect.String:
		return TILEDB_STRING_ASCII
	case reflect.Float32:
		return TILEDB_FLOAT32
	case reflect.Float64:
		return TILEDB_FLOAT64
	case reflect.Int8:
		return TILEDB_INT8
	case reflect.Int16:
		return TILEDB_INT16
	case reflect.Int32:
		return TILEDB_INT32
	case reflect.Int64:
		return TILEDB_INT64
	case reflect.Uint8:
		return TILEDB_UINT8
	case reflect.Uint16:
		return TILEDB_UINT16
	case reflect.Uint32:
		return TILEDB_UINT32
	case reflect.Uint64:
		return TILEDB_UINT64
	case reflect.Bool:
		return TILEDB_BOOL
	default:
		panic("can't get here")
	}
}

// NewEnumeration creates an enumeration with name and ordered or not values.
func NewEnumeration[T EnumerationType](tdbCtx *Context, name string, ordered bool, values []T) (*Enumeration, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cOrdered C.int
	if ordered {
		cOrdered = 1
	}

	tiledbType := enumerationTypeToTileDB[T]()
	var cCellNum C.uint32_t
	var cData unsafe.Pointer
	var cDataLen C.uint64_t
	var cOffsets unsafe.Pointer
	var cOffsetsLen C.uint64_t
	if tiledbType == TILEDB_STRING_ASCII {
		var dataSize int
		for _, v := range values {
			dataSize += reflect.ValueOf(v).Len()
		}
		data := make([]byte, 0, dataSize)
		offsets := make([]uint64, 0, len(values))
		defer runtime.KeepAlive(data)
		defer runtime.KeepAlive(offsets)
		var currOffset uint64
		for _, v := range values {
			data = append(data, reflect.ValueOf(v).String()...)
			offsets = append(offsets, currOffset)
			currOffset += uint64(reflect.ValueOf(v).Len())
		}
		cCellNum = C.uint32_t(TILEDB_VAR_NUM)
		cData = reflect.ValueOf(data).UnsafePointer()
		cDataLen = C.uint64_t(dataSize)
		cOffsets = reflect.ValueOf(offsets).UnsafePointer()
		cOffsetsLen = C.uint64_t(len(values) * int(reflect.TypeOf(uint64(0)).Size()))
	} else {
		var zz T
		cCellNum = C.uint32_t(1)
		cData = reflect.ValueOf(values).UnsafePointer()
		cDataLen = C.uint64_t(len(values) * int(reflect.TypeOf(zz).Size()))
	}

	var tiledbEnum *C.tiledb_enumeration_t
	ret := C.tiledb_enumeration_alloc(tdbCtx.tiledbContext, cName, C.tiledb_datatype_t(tiledbType), cCellNum, cOrdered,
		cData, cDataLen, cOffsets, cOffsetsLen, &tiledbEnum)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating enumeration: %s", tdbCtx.LastError())
	}

	e := &Enumeration{context: tdbCtx, tiledbEnum: tiledbEnum}
	freeOnGC(e)

	runtime.KeepAlive(values)

	return e, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (e *Enumeration) Free() {
	if e != nil && e.tiledbEnum != nil {
		C.tiledb_enumeration_free(&e.tiledbEnum)
	}
}

// Name returns the name of the enumeration
func (e *Enumeration) Name() (string, error) {
	var str *C.tiledb_string_t

	ret := C.tiledb_enumeration_get_name(e.context.tiledbContext, e.tiledbEnum, &str)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting name: %s", e.context.LastError())
	}

	var cName *C.char
	var cNameSize C.size_t
	ret = C.tiledb_string_view(str, &cName, &cNameSize)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting name: %s", e.context.LastError())
	}

	return C.GoStringN(cName, C.int(cNameSize)), nil
}

// Type returns the TileDB type of the enumeration
func (e *Enumeration) Type() (Datatype, error) {
	var attrType C.tiledb_datatype_t

	ret := C.tiledb_enumeration_get_type(e.context.tiledbContext, e.tiledbEnum, &attrType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb enumeration type: %s", e.context.LastError())
	}

	return Datatype(attrType), nil
}

// Type returns the number of cells for each enumeration value. It is 1 except for strings which is TILEDB_VAR_NUM
func (e *Enumeration) CellValNum() (uint32, error) {
	var cellValNum C.uint32_t

	ret := C.tiledb_enumeration_get_cell_val_num(e.context.tiledbContext, e.tiledbEnum, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting enumeration cell val num: %s", e.context.LastError())
	}

	return uint32(cellValNum), nil
}

// IsOrdered returns whether the enumerations values are ordered. Ordered values can be used with comparison
// operators in QueryConditions. Non-ordered values can be tested only for equality.
func (e *Enumeration) IsOrdered() (bool, error) {
	var ordered C.int

	ret := C.tiledb_enumeration_get_ordered(e.context.tiledbContext, e.tiledbEnum, &ordered)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error getting ordered: %s", e.context.LastError())
	}

	return ordered > 0, nil
}

// DumpSTDOUT() writes a human readable description of the enumeration in os.Stdout
func (e *Enumeration) DumpSTDOUT() error {
	ret := C.tiledb_enumeration_dump(e.context.tiledbContext, e.tiledbEnum, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping enumeration to stdout: %s", e.context.LastError())
	}

	return nil
}

// Dump created the file at path, (must not exist) and writes a human readable description of the enumeration
func (e *Enumeration) Dump(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("Error path already %s exists", path)
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	ret := C.tiledb_enumeration_dump(e.context.tiledbContext, e.tiledbEnum, cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping enumeration to file %s: %s", path, e.context.LastError())
	}

	return nil
}

// Values returns the enumeration values. The returned interface is a slice guaranteed to be cast to the type of the enumeration
func (e *Enumeration) Values() (interface{}, error) {
	typ, err := e.Type()
	if err != nil {
		return nil, err
	}

	var cData unsafe.Pointer
	var cDataSize C.uint64_t
	ret := C.tiledb_enumeration_get_data(e.context.tiledbContext, e.tiledbEnum, &cData, &cDataSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting data: %s", e.context.LastError())
	}

	if typ != TILEDB_STRING_ASCII {
		switch typ {
		case TILEDB_BOOL:
			return unsafe.Slice((*bool)(cData), cDataSize), nil
		case TILEDB_INT8:
			return unsafe.Slice((*int8)(cData), cDataSize), nil
		case TILEDB_INT16:
			return unsafe.Slice((*int16)(cData), cDataSize/C.uint64_t(2)), nil
		case TILEDB_INT32:
			return unsafe.Slice((*int32)(cData), cDataSize/C.uint64_t(4)), nil
		case TILEDB_INT64:
			return unsafe.Slice((*int64)(cData), cDataSize/C.uint64_t(8)), nil
		case TILEDB_UINT8:
			return unsafe.Slice((*uint8)(cData), cDataSize), nil
		case TILEDB_UINT16:
			return unsafe.Slice((*uint16)(cData), cDataSize/C.uint64_t(2)), nil
		case TILEDB_UINT32:
			return unsafe.Slice((*uint32)(cData), cDataSize/C.uint64_t(4)), nil
		case TILEDB_UINT64:
			return unsafe.Slice((*uint64)(cData), cDataSize/C.uint64_t(8)), nil
		case TILEDB_FLOAT32:
			return unsafe.Slice((*float32)(cData), cDataSize/C.uint64_t(4)), nil
		case TILEDB_FLOAT64:
			return unsafe.Slice((*float64)(cData), cDataSize/C.uint64_t(8)), nil
		default:
			panic("can't get here")
		}
	}

	var cOffsets unsafe.Pointer
	var cOffsetsSize C.uint64_t
	ret = C.tiledb_enumeration_get_offsets(e.context.tiledbContext, e.tiledbEnum, &cOffsets, &cOffsetsSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting data offsets: %s", e.context.LastError())
	}

	if int(cOffsetsSize)%8 > 0 {
		return nil, fmt.Errorf("Error getting data offsets: returned size does not contains an integer size of items")
	}

	var strs []string
	chars := unsafe.Slice((*byte)(cData), int(cDataSize))
	offs := unsafe.Slice((*C.uint64_t)(cOffsets), int(cOffsetsSize)/8)

	for i := 0; i < len(offs); i++ {
		var strLen int
		if i == len(offs)-1 {
			strLen = int(cDataSize - offs[i])
		} else {
			strLen = int(offs[i+1] - offs[i])
		}
		start := int(offs[i])
		strs = append(strs, string(chars[start:start+strLen]))
	}

	return strs, nil
}

// AddEnumeration adds the Enumeration to the schema. It must be added before we add it to an attribute.
func (a *ArraySchema) AddEnumeration(e *Enumeration) error {
	ret := C.tiledb_array_schema_add_enumeration(a.context.tiledbContext, a.tiledbArraySchema, e.tiledbEnum)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding enumeration: %s", a.context.LastError())
	}

	return nil
}

// LoadAllEnumeration is for use with TileDB cloud arrays. It fetches the enumeration values from the server.
// The method is called ondemand if the client tries to fetch enumeration values for a tiledb:// array.
func (a *Array) LoadAllEnumerations() error {
	ret := C.tiledb_array_load_all_enumerations(a.context.tiledbContext, a.tiledbArray)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error loading all enumerations: %s", a.context.LastError())
	}

	return nil
}

// GetEnumeration return the named Enumeration from the array schema
func (a *Array) GetEnumeration(name string) (*Enumeration, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var tiledbEnum *C.tiledb_enumeration_t
	ret := C.tiledb_array_get_enumeration(a.context.tiledbContext, a.tiledbArray, cName, &tiledbEnum)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting enumeration %s: %s", name, a.context.LastError())
	}

	return &Enumeration{context: a.context, tiledbEnum: tiledbEnum}, nil
}

// SetEnumerationName sets the enumeration for the attribute. The enumeration must be set to the
// schema and the attribute maximum size must fit the size of the enumeration values
func (a *Attribute) SetEnumerationName(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_attribute_set_enumeration_name(a.context.tiledbContext, a.tiledbAttribute, cName)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting enumeration name: %s", a.context.LastError())
	}

	return nil
}

// GetEnumerationName returns the enumeration name of the attribute
func (a *Attribute) GetEnumerationName() (string, error) {
	var str *C.tiledb_string_t

	ret := C.tiledb_attribute_get_enumeration_name(a.context.tiledbContext, a.tiledbAttribute, &str)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting enumeration name: %s", a.context.LastError())
	}

	var cName *C.char
	var cNameSize C.size_t
	ret = C.tiledb_string_view(str, &cName, &cNameSize)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting name: %s", a.context.LastError())
	}

	return C.GoStringN(cName, C.int(cNameSize)), nil
}

// UseEnumerations set true to allow query conditions with enumeration literals
func (qc *QueryCondition) UseEnumeration(useEnum bool) error {
	var cUseEnum C.int
	if useEnum {
		cUseEnum = 1
	}

	ret := C.tiledb_query_condition_set_use_enumeration(qc.context.tiledbContext, qc.cond, cUseEnum)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error toggling enumerations use: %s", qc.context.LastError())
	}

	return nil
}

// AddEnumeration adds enumeration to the schema evolution
func (ase *ArraySchemaEvolution) AddEnumeration(e *Enumeration) error {
	name, err := e.Name()
	if err != nil {
		return fmt.Errorf("Error getting enumeration name: %s", e.context.LastError())
	}

	ret := C.tiledb_array_schema_evolution_add_enumeration(ase.context.tiledbContext, ase.tiledbArraySchemaEvolution, e.tiledbEnum)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding enumeration %s to tiledb arraySchemaEvolution: %s", name, ase.context.LastError())
	}

	return nil
}

// DropEnumeration removes the enumeration from the schema evolution
func (ase *ArraySchemaEvolution) DropEnumeration(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_array_schema_evolution_drop_enumeration(ase.context.tiledbContext, ase.tiledbArraySchemaEvolution, cName)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dropping enumeration %s from tiledb arraySchemaEvolution: %s", name, ase.context.LastError())
	}

	return nil
}

// DeserializeLoadEnumerationsRequest deserializes a LoadEnumerationsRequests. This is used by TileDB-Cloud
func DeserializeLoadEnumerationsRequest(array *Array, serializationType SerializationType, request *Buffer) (*Buffer, error) {
	response, err := NewBuffer(array.context)
	if err != nil {
		return nil, fmt.Errorf("error deserializing load enumerations request: %s", array.context.LastError())
	}

	ret := C.tiledb_handle_load_enumerations_request(array.context.tiledbContext, array.tiledbArray, C.tiledb_serialization_type_t(serializationType),
		request.tiledbBuffer, response.tiledbBuffer)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error deserializing load enumerations request: %s", array.context.LastError())
	}

	runtime.KeepAlive(request)
	runtime.KeepAlive(array)

	return response, nil
}
