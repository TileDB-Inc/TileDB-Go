package tiledb

/*
#cgo LDFLAGS: -ltiledb
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// Attribute is tiledb attribute
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
		return nil, fmt.Errorf("Error creating tiledb attribute: %s", context.GetLastError())
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

// SetCompressor for attribute
func (a *Attribute) SetCompressor(compressor Compressor) error {
	ret := C.tiledb_attribute_set_compressor(a.context.tiledbContext, a.tiledbAttribute, C.tiledb_compressor_t(compressor.Compressor), C.int(compressor.Level))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb attribute compressor: %s", a.context.GetLastError())
	}
	return nil
}

// Compressor returns compressor for attribute
func (a *Attribute) Compressor() (*Compressor, error) {
	var compressor_t C.tiledb_compressor_t
	var clevel C.int
	ret := C.tiledb_attribute_get_compressor(a.context.tiledbContext, a.tiledbAttribute, &compressor_t, &clevel)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting tiledb attribute compressor: %s", a.context.GetLastError())
	}

	return &Compressor{Compressor: CompressorType(compressor_t), Level: int(clevel)}, nil
}

func (a *Attribute) SetCellValNum(val uint) error {
	ret := C.tiledb_attribute_set_cell_val_num(a.context.tiledbContext, a.tiledbAttribute, C.uint(val))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting tiledb attribute cell val num: %s", a.context.GetLastError())
	}
	return nil
}

// CellValNum returns compressor for attribute
func (a *Attribute) CellValNum() (uint, error) {
	var cellValNum C.uint
	ret := C.tiledb_attribute_get_cell_val_num(a.context.tiledbContext, a.tiledbAttribute, &cellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute cell val num: %s", a.context.GetLastError())
	}

	return uint(cellValNum), nil
}

// Name returns name of attribute
func (a *Attribute) Name() (string, error) {
	var cName *C.char
	defer C.free(unsafe.Pointer(cName))
	ret := C.tiledb_attribute_get_name(a.context.tiledbContext, a.tiledbAttribute, &cName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting tiledb attribute name: %s", a.context.GetLastError())
	}

	return C.GoString(cName), nil
}

// Type returns the attribute datatype
func (a *Attribute) Type() (Datatype, error) {
	var attrType C.tiledb_datatype_t
	ret := C.tiledb_attribute_get_type(a.context.tiledbContext, a.tiledbAttribute, &attrType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb attribute type: %s", a.context.GetLastError())
	}
	return Datatype(attrType), nil
}
