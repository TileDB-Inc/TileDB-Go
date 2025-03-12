package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"unsafe"
)

type enumerationHandle struct{ *capiHandle }

func freeCapiEnumeration(c unsafe.Pointer) {
	C.tiledb_enumeration_free((**C.tiledb_enumeration_t)(unsafe.Pointer(&c)))
}

func newEnumerationHandle(ptr *C.tiledb_enumeration_t) enumerationHandle {
	return enumerationHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiEnumeration)}
}

func (x enumerationHandle) Get() *C.tiledb_enumeration_t {
	return (*C.tiledb_enumeration_t)(x.capiHandle.Get())
}

// Enumeration is a TileDB enumeration for Attributes
type Enumeration struct {
	context    *Context
	tiledbEnum enumerationHandle
}

func newEnumerationFromHandle(tdbCtx *Context, handle enumerationHandle) *Enumeration {
	return &Enumeration{context: tdbCtx, tiledbEnum: handle}
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

// NewOrderedEnumeration creates an ordered enumeration with name and values.
func NewOrderedEnumeration[T EnumerationType](tdbCtx *Context, name string, values []T) (*Enumeration, error) {
	return newEnumeration(tdbCtx, name, true, values)
}

// NewOrderedEnumeration creates an unordered enumeration with name and values.
func NewUnorderedEnumeration[T EnumerationType](tdbCtx *Context, name string, values []T) (*Enumeration, error) {
	return newEnumeration(tdbCtx, name, false, values)
}

// newEnumeration creates an enumeration with name and ordered or not values.
func newEnumeration[T EnumerationType](tdbCtx *Context, name string, ordered bool, values []T) (*Enumeration, error) {
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

	// for empty enumerations, TileDB accepts only nils, not empty slices
	if len(values) == 0 {
		if tiledbType == TILEDB_STRING_ASCII {
			cCellNum = C.uint32_t(TILEDB_VAR_NUM)
		} else {
			cCellNum = C.uint32_t(1)
		}
	} else if tiledbType == TILEDB_STRING_ASCII {
		var dataSize int
		for _, v := range values {
			dataSize += reflect.ValueOf(v).Len()
		}
		data := make([]byte, 0, dataSize)
		offsets := make([]uint64, 0, len(values))
		var currOffset uint64
		for _, v := range values {
			data = append(data, reflect.ValueOf(v).String()...)
			offsets = append(offsets, currOffset)
			currOffset += uint64(reflect.ValueOf(v).Len())
		}
		cCellNum = C.uint32_t(TILEDB_VAR_NUM)
		cData = unsafe.Pointer(unsafe.SliceData(data))
		cDataLen = C.uint64_t(dataSize)
		cOffsets = unsafe.Pointer(unsafe.SliceData(offsets))
		cOffsetsLen = C.uint64_t(len(values) * int(unsafe.Sizeof(uint64(0))))
	} else {
		var zz T
		cCellNum = C.uint32_t(1)
		cData = unsafe.Pointer(unsafe.SliceData(values))
		cDataLen = C.uint64_t(len(values) * int(unsafe.Sizeof(zz)))
	}

	var tiledbEnum *C.tiledb_enumeration_t
	ret := C.tiledb_enumeration_alloc(tdbCtx.tiledbContext.Get(), cName, C.tiledb_datatype_t(tiledbType), cCellNum, cOrdered,
		cData, cDataLen, cOffsets, cOffsetsLen, &tiledbEnum)
	// cData and cOffsets are kept alive by passing them to cgo call.
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating enumeration: %w", tdbCtx.LastError())
	}

	return newEnumerationFromHandle(tdbCtx, newEnumerationHandle(tiledbEnum)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (e *Enumeration) Free() {
	e.tiledbEnum.Free()
}

// Name returns the name of the enumeration.
func (e *Enumeration) Name() (string, error) {
	var str *C.tiledb_string_t

	ret := C.tiledb_enumeration_get_name(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &str)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting name: %w", e.context.LastError())
	}
	defer C.tiledb_string_free(&str)

	return stringHandleToString(str)
}

// Type returns the TileDB type of the enumeration.
func (e *Enumeration) Type() (Datatype, error) {
	var attrType C.tiledb_datatype_t

	ret := C.tiledb_enumeration_get_type(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &attrType)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb enumeration type: %w", e.context.LastError())
	}

	return Datatype(attrType), nil
}

// Type returns the number of cells for each enumeration value. It is 1 except for strings which is TILEDB_VAR_NUM.
func (e *Enumeration) CellValNum() (uint32, error) {
	var cellValNum C.uint32_t

	ret := C.tiledb_enumeration_get_cell_val_num(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &cellValNum)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting enumeration cell val num: %w", e.context.LastError())
	}

	return uint32(cellValNum), nil
}

// IsOrdered returns whether the enumerations values are ordered. Ordered values can be used with comparison
// operators in QueryConditions. Non-ordered values can be tested only for equality.
func (e *Enumeration) IsOrdered() (bool, error) {
	var ordered C.int

	ret := C.tiledb_enumeration_get_ordered(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &ordered)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error getting ordered: %w", e.context.LastError())
	}

	return ordered > 0, nil
}

// DumpSTDOUT writes a human-readable description of the enumeration to os.Stdout.
func (e *Enumeration) DumpSTDOUT() error {
	ret := C.tiledb_enumeration_dump(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), C.stdout)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping enumeration to stdout: %w", e.context.LastError())
	}

	return nil
}

// Dump creates the file at path (must not exist) and writes a human-readable description of the enumeration.
func (e *Enumeration) Dump(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	ret := C.tiledb_enumeration_dump(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), cFile)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping enumeration to file %s: %w", path, e.context.LastError())
	}

	return nil
}

// Values returns the enumeration values. The returned interface is a slice guaranteed to be cast to the type of the enumeration.
func (e *Enumeration) Values() (interface{}, error) {
	typ, err := e.Type()
	if err != nil {
		return nil, err
	}

	var cData unsafe.Pointer
	var cDataSize C.uint64_t
	ret := C.tiledb_enumeration_get_data(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &cData, &cDataSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting data: %w", e.context.LastError())
	}

	if typ != TILEDB_STRING_ASCII {
		switch typ {
		case TILEDB_BOOL:
			return copyUnsafeSliceOfEnumerationValues[bool](cData, int(cDataSize))
		case TILEDB_INT8:
			return copyUnsafeSliceOfEnumerationValues[int8](cData, int(cDataSize))
		case TILEDB_INT16:
			return copyUnsafeSliceOfEnumerationValues[int16](cData, int(cDataSize))
		case TILEDB_INT32:
			return copyUnsafeSliceOfEnumerationValues[int32](cData, int(cDataSize))
		case TILEDB_INT64:
			return copyUnsafeSliceOfEnumerationValues[int64](cData, int(cDataSize))
		case TILEDB_UINT8:
			return copyUnsafeSliceOfEnumerationValues[uint8](cData, int(cDataSize))
		case TILEDB_UINT16:
			return copyUnsafeSliceOfEnumerationValues[uint16](cData, int(cDataSize))
		case TILEDB_UINT32:
			return copyUnsafeSliceOfEnumerationValues[uint32](cData, int(cDataSize))
		case TILEDB_UINT64:
			return copyUnsafeSliceOfEnumerationValues[uint64](cData, int(cDataSize))
		case TILEDB_FLOAT32:
			return copyUnsafeSliceOfEnumerationValues[float32](cData, int(cDataSize))
		case TILEDB_FLOAT64:
			return copyUnsafeSliceOfEnumerationValues[float64](cData, int(cDataSize))
		default:
			panic("can't get here")
		}
	}

	var cOffsets unsafe.Pointer
	var cOffsetsSize C.uint64_t
	ret = C.tiledb_enumeration_get_offsets(e.context.tiledbContext.Get(), e.tiledbEnum.Get(), &cOffsets, &cOffsetsSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting data offsets: %w", e.context.LastError())
	}

	if int(cOffsetsSize)%8 > 0 {
		return nil, errors.New("error getting data offsets: returned size does not contain an integer size of items")
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

	runtime.KeepAlive(e)
	return strs, nil
}

// ExtendEnumeration extends an existing enumeration to add more values. The returned value should be
// used with ArraySchemaEvolution.ApplyExtendedEnumeration to make changes persistent.
func ExtendEnumeration[T EnumerationType](tdbCtx *Context, e *Enumeration, values []T) (*Enumeration, error) {
	if len(values) == 0 {
		return nil, errors.New("error extending enumeration: empty values")
	}

	eName, err := e.Name()
	if err != nil {
		return nil, fmt.Errorf("error extending enumeration: failed to get name of enumeration: %w", tdbCtx.LastError())
	}

	eType, err := e.Type()
	if err != nil {
		return nil, fmt.Errorf("error extending enumeration: failed to get type of enumeration %s: %w", eName, tdbCtx.LastError())
	}

	tiledbType := enumerationTypeToTileDB[T]()
	if eType != tiledbType {
		return nil, fmt.Errorf("error extending enumeration: type mismatch: enumeration type %v, values type %v", eType, tiledbType)
	}

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
		var currOffset uint64
		for _, v := range values {
			data = append(data, reflect.ValueOf(v).String()...)
			offsets = append(offsets, currOffset)
			currOffset += uint64(reflect.ValueOf(v).Len())
		}
		cData = unsafe.Pointer(unsafe.SliceData(data))
		cDataLen = C.uint64_t(dataSize)
		cOffsets = unsafe.Pointer(unsafe.SliceData(offsets))
		cOffsetsLen = C.uint64_t(uintptr(len(values)) * unsafe.Sizeof(uint64(0)))
	} else {
		var zz T
		cData = unsafe.Pointer(unsafe.SliceData(values))
		cDataLen = C.uint64_t(uintptr(len(values)) * unsafe.Sizeof(zz))
	}

	var extEnum *C.tiledb_enumeration_t

	ret := C.tiledb_enumeration_extend(tdbCtx.tiledbContext.Get(), e.tiledbEnum.Get(), cData, cDataLen, cOffsets, cOffsetsLen, &extEnum)
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(e)
	// cData and cOffsets are being kept alive by passing them to cgo call.
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error extending enumeration: %w", tdbCtx.LastError())
	}

	return newEnumerationFromHandle(tdbCtx, newEnumerationHandle(extEnum)), nil
}

// AddEnumeration adds the Enumeration to the schema. It must be added before we add it to an attribute.
func (a *ArraySchema) AddEnumeration(e *Enumeration) error {
	ret := C.tiledb_array_schema_add_enumeration(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), e.tiledbEnum.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding enumeration: %w", a.context.LastError())
	}

	return nil
}

// EnumerationFromName gets an Enumeration from the ArraySchema by name
func (a *ArraySchema) EnumerationFromName(name string) (*Enumeration, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var enumPtr *C.tiledb_enumeration_t
	ret := C.tiledb_array_schema_get_enumeration_from_name(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), cName, &enumPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting enumeration from name: %w", a.context.LastError())
	}

	return newEnumerationFromHandle(a.context, newEnumerationHandle(enumPtr)), nil
}

// EnumerationFromName gets an Enumeration from the ArraySchema by its Attribute name.
func (a *ArraySchema) EnumerationFromAttributeName(name string) (*Enumeration, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	var enumPtr *C.tiledb_enumeration_t
	ret := C.tiledb_array_schema_get_enumeration_from_attribute_name(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), cName, &enumPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting enumeration from attribute name: %w", a.context.LastError())
	}

	return newEnumerationFromHandle(a.context, newEnumerationHandle(enumPtr)), nil
}

// LoadAllEnumeration is for use with TileDB cloud arrays. It fetches the enumeration values from the server.
// The method is called ondemand if the client tries to fetch enumeration values for a tiledb:// array.
func (a *Array) LoadAllEnumerations() error {
	ret := C.tiledb_array_load_all_enumerations(a.context.tiledbContext.Get(), a.tiledbArray.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error loading all enumerations: %w", a.context.LastError())
	}

	return nil
}

// LoadEnumerationsAllSchemas is for use with TileDB cloud arrays. It fetches the enumeration values from the server for all array schemas, past and present.
func (a *Array) LoadEnumerationsAllSchemas() error {
	ret := C.tiledb_array_load_enumerations_all_schemas(a.context.tiledbContext.Get(), a.tiledbArray.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error loading enumerations for all schemas: %w", a.context.LastError())
	}

	return nil
}

// GetEnumeration return the named Enumeration from the array schema.
func (a *Array) GetEnumeration(name string) (*Enumeration, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var tiledbEnum *C.tiledb_enumeration_t
	ret := C.tiledb_array_get_enumeration(a.context.tiledbContext.Get(), a.tiledbArray.Get(), cName, &tiledbEnum)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting enumeration %s: %w", name, a.context.LastError())
	}

	return newEnumerationFromHandle(a.context, newEnumerationHandle(tiledbEnum)), nil
}

// SetEnumerationName sets the enumeration for the attribute. The enumeration must be set to the
// schema and the attribute maximum size must fit the size of the enumeration values.
func (a *Attribute) SetEnumerationName(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_attribute_set_enumeration_name(a.context.tiledbContext.Get(), a.tiledbAttribute.Get(), cName)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting enumeration name: %w", a.context.LastError())
	}

	return nil
}

// GetEnumerationName returns the enumeration name of the attribute.
func (a *Attribute) GetEnumerationName() (string, error) {
	var str *C.tiledb_string_t

	ret := C.tiledb_attribute_get_enumeration_name(a.context.tiledbContext.Get(), a.tiledbAttribute.Get(), &str)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting enumeration name: %w", a.context.LastError())
	}
	defer C.tiledb_string_free(&str)

	return stringHandleToString(str)
}

// UseEnumerations set true to allow query conditions with enumeration literals.
func (qc *QueryCondition) UseEnumeration(useEnum bool) error {
	var cUseEnum C.int
	if useEnum {
		cUseEnum = 1
	}

	ret := C.tiledb_query_condition_set_use_enumeration(qc.context.tiledbContext.Get(), qc.cond, cUseEnum)
	runtime.KeepAlive(qc)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error toggling enumerations use: %w", qc.context.LastError())
	}

	return nil
}

// AddEnumeration adds enumeration to the schema evolution.
func (ase *ArraySchemaEvolution) AddEnumeration(e *Enumeration) error {
	name, err := e.Name()
	if err != nil {
		return fmt.Errorf("error getting enumeration name: %w", e.context.LastError())
	}

	ret := C.tiledb_array_schema_evolution_add_enumeration(ase.context.tiledbContext.Get(), ase.tiledbArraySchemaEvolution.Get(), e.tiledbEnum.Get())
	runtime.KeepAlive(ase)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding enumeration %s to tiledb arraySchemaEvolution: %w", name, ase.context.LastError())
	}

	return nil
}

// DropEnumeration removes the enumeration from the schema evolution.
func (ase *ArraySchemaEvolution) DropEnumeration(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_array_schema_evolution_drop_enumeration(ase.context.tiledbContext.Get(), ase.tiledbArraySchemaEvolution.Get(), cName)
	runtime.KeepAlive(ase)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dropping enumeration %s from tiledb arraySchemaEvolution: %w", name, ase.context.LastError())
	}

	return nil
}

// ApplyExtendedEnumeration applies to the schema evolution the result of ExtendEnumeration.
func (ase *ArraySchemaEvolution) ApplyExtendedEnumeration(e *Enumeration) error {
	ret := C.tiledb_array_schema_evolution_extend_enumeration(ase.context.tiledbContext.Get(), ase.tiledbArraySchemaEvolution.Get(), e.tiledbEnum.Get())
	runtime.KeepAlive(ase)
	runtime.KeepAlive(e)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error applying extended enumeration to arraySchemaEvolution: %w", ase.context.LastError())
	}

	return nil
}

// copyUnsafeSliceOfEnumerationValues copies the values returned by tiledb_enumeration_get_data to a slice
// in go managed memory. This is for safety because the returned data points to unsafe memory handled by core.
// The tiledb_enumeration_get_data returns the aggregated size (sth like len() * sizeOf) so this methods
// also calculaces the data size per type
func copyUnsafeSliceOfEnumerationValues[T any](data unsafe.Pointer, dataSize int) ([]T, error) {
	var zero T
	factor := int(unsafe.Sizeof(zero))
	if dataSize%factor > 0 {
		return nil, errors.New("error getting data values: returned size does not contains an integer size of items")
	}

	retLen := dataSize / factor
	ret := make([]T, retLen)
	copy(ret, unsafe.Slice((*T)(data), retLen))
	return ret, nil
}
