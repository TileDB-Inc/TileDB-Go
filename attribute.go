package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
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
func (a *Attribute) CellValNum() (uint, error) {
	var cellValNum C.uint32_t
	ret := C.tiledb_attribute_get_cell_val_num(a.context.tiledbContext, a.tiledbAttribute, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute cell val num: %s", a.context.LastError())
	}

	return uint(cellValNum), nil
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
