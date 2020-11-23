package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
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

/*
Attribute describes an attribute of an Array cell.

An attribute specifies a name and datatype for a particular value in each array cell. There are 3 supported attribute types:

    Fundamental types, such as char, int, double, uint64, etc..
    Fixed sized arrays: [N]T or make([]T, N), where T is a fundamental type
    Variable length data: string, []T, where T is a fundamental type
*/
type Attribute struct {
	tiledbAttribute *C.tiledb_attribute_t
	context         *Context
}

// NewAttribute alloc a new attribute
func NewAttribute(context *Context, name string, datatype Datatype) (*Attribute, error) {
	attribute := Attribute{context: context}
	var cname *C.char = C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	ret := C.tiledb_attribute_alloc(context.tiledbContext, cname, C.tiledb_datatype_t(datatype), &attribute.tiledbAttribute)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb attribute: %s", context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&attribute, func(attribute *Attribute) {
		attribute.Free()
	})

	return &attribute, nil
}

// Free tiledb_attribute_t that was allocated on heap in c
func (a *Attribute) Free() {
	if a.tiledbAttribute != nil {
		C.tiledb_attribute_free(&a.tiledbAttribute)
	}
}

// SetFilterList sets the attribute filterList
func (a *Attribute) SetFilterList(filterlist *FilterList) error {
	ret := C.tiledb_attribute_set_filter_list(a.context.tiledbContext, a.tiledbAttribute, filterlist.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb attribute filter list: %s", a.context.LastError())
	}
	return nil
}

// FilterList returns a copy of the filter list for attribute
func (a *Attribute) FilterList() (*FilterList, error) {
	filterList := FilterList{context: a.context}
	ret := C.tiledb_attribute_get_filter_list(a.context.tiledbContext, a.tiledbAttribute, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb attribute filter list: %s", a.context.LastError())
	}

	return &filterList, nil
}

// SetCellValNum Sets the number of attribute values per cell.
// This is inferred from the type parameter of the NewAttribute
// function, but can also be set manually.
func (a *Attribute) SetCellValNum(val uint) error {
	ret := C.tiledb_attribute_set_cell_val_num(a.context.tiledbContext,
		a.tiledbAttribute, C.uint32_t(val))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb attribute cell val num: %s", a.context.LastError())
	}
	return nil
}

// CellValNum returns number of values of one cell on this attribute.
// For variable-sized attributes returns TILEDB_VAR_NUM.
func (a *Attribute) CellValNum() (uint32, error) {
	var cellValNum C.uint32_t
	ret := C.tiledb_attribute_get_cell_val_num(a.context.tiledbContext, a.tiledbAttribute, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute cell val num: %s", a.context.LastError())
	}

	return uint32(cellValNum), nil
}

// CellSize gets attribute cell size
func (a *Attribute) CellSize() (uint64, error) {
	var cellSize C.uint64_t
	ret := C.tiledb_attribute_get_cell_size(a.context.tiledbContext, a.tiledbAttribute, &cellSize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute cell size: %s", a.context.LastError())
	}

	return uint64(cellSize), nil
}

// Sets the default fill value for the input attribute. This value will
// be used for the input attribute whenever querying (1) an empty cell in
// a dense array, or (2) a non-empty cell (in either dense or sparse array)
// when values on the input attribute are missing (e.g., if the user writes
// a subset of the attributes in a write operation).
// Applicable to var-sized attributes.
// @note A call to `tiledb_attribute_cell_val_num` sets the fill value
//      of the attribute to its default. Therefore, make sure you invoke
//      `tiledb_attribute_set_fill_value` after deciding on the number
//      of values this attribute will hold in each cell.
// @note For fixed-sized attributes, the input `size` should be equal
//      to the cell size.
func (a *Attribute) SetFillValue(value interface{}) error {

	if value == nil {
		return errors.New("Unrecognized value type passed: Cannot be a nil")
	}

	if reflect.TypeOf(value).Kind() == reflect.Slice {
		return errors.New("Unrecognized value type passed: Cannot be a slice")
	}

	valueType := reflect.TypeOf(value).Kind()

	cellValNum, err := a.CellValNum()
	if err != nil {
		return err
	}

	attrDataType, err := a.Type()
	if err != nil {
		return err
	}

	var valueSize C.uint64_t
	if cellValNum == uint32(TILEDB_VAR_NUM) {
		valueSize = C.uint64_t(reflect.TypeOf(value).Size())
	} else {
		valueSize = C.uint64_t(attrDataType.Size() * uint64(cellValNum))
	}

	var ret C.int32_t
	switch valueType {
	case reflect.Int:
		tmpValue := value.(int)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Int8:
		tmpValue := value.(int8)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Int16:
		tmpValue := value.(int16)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Int32:
		tmpValue := value.(int32)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Int64:
		tmpValue := value.(int64)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Uint:
		tmpValue := value.(uint)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Uint8:
		tmpValue := value.(uint8)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Uint16:
		tmpValue := value.(uint16)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Uint32:
		tmpValue := value.(uint32)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Uint64:
		tmpValue := value.(uint64)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Float32:
		tmpValue := value.(float32)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.Float64:
		tmpValue := value.(float64)
		ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(&tmpValue), valueSize)
	case reflect.String:
		stringValue := value.(string)
		valueSize = C.uint64_t(len(stringValue))
		cTmpValue := C.CString(stringValue)
		defer C.free(unsafe.Pointer(cTmpValue))
		if valueSize > 0 {
			ret = C.tiledb_attribute_set_fill_value(a.context.tiledbContext, a.tiledbAttribute, unsafe.Pointer(cTmpValue), valueSize)
		}
	default:
		valueInterfaceVal := reflect.ValueOf(value)
		return fmt.Errorf("Unrecognized value type passed: %s", valueInterfaceVal.Kind().String())
	}

	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error filling attribute value: %s", a.context.LastError())
	}

	return nil
}

// Gets the default fill value for the input attribute. This value will
// be used for the input attribute whenever querying (1) an empty cell in
// a dense array, or (2) a non-empty cell (in either dense or sparse array)
// when values on the input attribute are missing (e.g., if the user writes
// a subset of the attributes in a write operation).
// Applicable to both fixed-sized and var-sized attributes.
func (a *Attribute) GetFillValue() (interface{}, uint64, error) {
	var fillValueSize C.uint64_t
	var cvalue unsafe.Pointer

	ret := C.tiledb_attribute_get_fill_value(a.context.tiledbContext, a.tiledbAttribute, &cvalue, &fillValueSize)
	if ret != C.TILEDB_OK {
		return nil, 0, fmt.Errorf("Error getting tiledb attribute fill value: %s", a.context.LastError())
	}

	attrDataType, err := a.Type()
	if err != nil {
		return nil, 0, fmt.Errorf("Error getting tiledb attribute fill value: %s", a.context.LastError())
	}

	value, err := attrDataType.GetValue(1, cvalue)
	if err != nil {
		return nil, 0, fmt.Errorf("Error getting tiledb attribute fill value: %s", a.context.LastError())
	}

	return value, uint64(fillValueSize), nil
}

// Name returns name of attribute
func (a *Attribute) Name() (string, error) {
	var cName *C.char
	defer C.free(unsafe.Pointer(cName))
	ret := C.tiledb_attribute_get_name(a.context.tiledbContext, a.tiledbAttribute, &cName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting tiledb attribute name: %s", a.context.LastError())
	}

	return C.GoString(cName), nil
}

// Type returns the attribute datatype
func (a *Attribute) Type() (Datatype, error) {
	var attrType C.tiledb_datatype_t
	ret := C.tiledb_attribute_get_type(a.context.tiledbContext, a.tiledbAttribute, &attrType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute type: %s", a.context.LastError())
	}
	return Datatype(attrType), nil
}

// DumpSTDOUT Dumps the attribute in ASCII format to stdout
func (a *Attribute) DumpSTDOUT() error {
	ret := C.tiledb_attribute_dump(a.context.tiledbContext, a.tiledbAttribute, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping attribute to stdout: %s", a.context.LastError())
	}
	return nil
}

// Dump Dumps the attribute in ASCII format in the selected output.
func (a *Attribute) Dump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("Error path already %s exists", path)
	}

	// Convert to char *
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Set mode as char*
	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	// Open file to get FILE*
	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	// Dump attribute to file
	ret := C.tiledb_attribute_dump(a.context.tiledbContext, a.tiledbAttribute, cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping attribute to file %s: %s", path, a.context.LastError())
	}
	return nil
}
