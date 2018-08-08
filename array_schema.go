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
ArraySchema Schema describing an array.

The schema is an independent description of an array. A schema can be used to create multiple array’s, and stores information about its domain, cell types, and compression details. An array schema is composed of:

    A Domain
    A set of Attributes
    Memory layout definitions: tile and cell
    Compression details for Array level factors like offsets and coordinates
*/
type ArraySchema struct {
	tiledbArraySchema *C.tiledb_array_schema_t
	context           *Context
}

// MarshalJSON marshal arraySchema struct to json using tiledb
func (a *ArraySchema) MarshalJSON() ([]byte, error) {
	var jsonString *C.char
	defer C.free(unsafe.Pointer(jsonString))
	var jsonStringLength C.uint64_t
	ret := C.tiledb_array_schema_serialize(a.context.tiledbContext, a.tiledbArraySchema, C.tiledb_serialization_type_t(TILEDB_JSON), &jsonString, &jsonStringLength)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error marshaling json for array schema: %s", a.context.LastError())
	}
	return []byte(C.GoString(jsonString)), nil
}

// UnmarshalJSON marshal arraySchema struct to json using tiledb
func (a *ArraySchema) UnmarshalJSON(b []byte) error {
	var err error
	if a.context == nil {
		a.context, err = NewContext(nil)
		if err != nil {
			return err
		}
	}
	jsonString := C.CString(string(b))
	defer C.free(unsafe.Pointer(jsonString))
	var jsonStringLength = C.uint64_t(len(b))
	ret := C.tiledb_array_schema_deserialize(a.context.tiledbContext, &a.tiledbArraySchema, C.tiledb_serialization_type_t(TILEDB_JSON), jsonString, jsonStringLength)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error unmarshaling json for array schema: %s", a.context.LastError())
	}
	return nil
}

// NewArraySchema alloc a new ArraySchema
func NewArraySchema(ctx *Context, arrayType ArrayType) (*ArraySchema, error) {
	arraySchema := ArraySchema{context: ctx}
	ret := C.tiledb_array_schema_alloc(arraySchema.context.tiledbContext, C.tiledb_array_type_t(arrayType), &arraySchema.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb arraySchema: %s", arraySchema.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&arraySchema, func(arraySchema *ArraySchema) {
		arraySchema.Free()
	})

	return &arraySchema, nil
}

// Free tiledb_array_schema_t that was allocated on heap in c
func (a *ArraySchema) Free() {
	if a.tiledbArraySchema != nil {
		C.tiledb_array_schema_free(&a.tiledbArraySchema)
	}
}

// AddAttributes add one or more attributes to the array
func (a *ArraySchema) AddAttributes(attributes ...*Attribute) error {
	for _, attribute := range attributes {
		ret := C.tiledb_array_schema_add_attribute(a.context.tiledbContext, a.tiledbArraySchema, attribute.tiledbAttribute)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error adding attributes to tiledb arraySchema: %s", a.context.LastError())
		}
	}
	return nil
}

// AttributeNum returns the number of attributes
func (a *ArraySchema) AttributeNum() (uint, error) {
	var attrNum C.uint32_t
	ret := C.tiledb_array_schema_get_attribute_num(a.context.tiledbContext, a.tiledbArraySchema, &attrNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting attribute number for tiledb arraySchema: %s", a.context.LastError())
	}
	return uint(attrNum), nil
}

// AttributeFromIndex get a copy of an Attribute in the schema by name.
func (a *ArraySchema) AttributeFromIndex(index uint) (*Attribute, error) {
	attr := Attribute{context: a.context}
	ret := C.tiledb_array_schema_get_attribute_from_index(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		C.uint32_t(index),
		&attr.tiledbAttribute)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting attribute %d for tiledb arraySchema: %s", index, a.context.LastError())
	}
	return &attr, nil
}

// AttributeFromName Get a copy of an Attribute in the schema by index.
// Attributes are ordered the same way they were defined when
// constructing the array schema.
func (a *ArraySchema) AttributeFromName(attrName string) (*Attribute, error) {
	cAttrName := C.CString(attrName)
	defer C.free(unsafe.Pointer(cAttrName))
	attr := Attribute{context: a.context}
	ret := C.tiledb_array_schema_get_attribute_from_name(a.context.tiledbContext, a.tiledbArraySchema, cAttrName, &attr.tiledbAttribute)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting attribute %s for tiledb arraySchema: %s", attrName, a.context.LastError())
	}
	return &attr, nil
}

// Attributes gets all attributes in the array.
func (a *ArraySchema) Attributes() ([]*Attribute, error) {
	attributes := make([]*Attribute, 0)

	attrNum, err := a.AttributeNum()
	if err != nil {
		return nil, fmt.Errorf("Error getting AttributeNum: %s", err)
	}

	for i := uint(0); i < attrNum; i++ {
		attribute, err := a.AttributeFromIndex(i)
		if err != nil {
			return nil, fmt.Errorf("Error getting Attribute: %s", err)
		}
		attributes = append(attributes, attribute)
	}
	return attributes, nil
}

// SetDomain sets the array domain
func (a *ArraySchema) SetDomain(domain *Domain) error {
	ret := C.tiledb_array_schema_set_domain(a.context.tiledbContext, a.tiledbArraySchema, domain.tiledbDomain)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting domain for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// Domain returns the array's domain
func (a *ArraySchema) Domain() (*Domain, error) {
	domain := Domain{context: a.context}
	ret := C.tiledb_array_schema_get_domain(a.context.tiledbContext, a.tiledbArraySchema, &domain.tiledbDomain)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error setting domain for tiledb arraySchema: %s", a.context.LastError())
	}
	return &domain, nil
}

// SetCapacity sets the tile capacity.
func (a *ArraySchema) SetCapacity(capacity uint64) error {
	ret := C.tiledb_array_schema_set_capacity(a.context.tiledbContext, a.tiledbArraySchema, C.uint64_t(capacity))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting capacity for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// Capacity returns the tile capacity.
func (a *ArraySchema) Capacity() (uint64, error) {
	var capacity C.uint64_t
	ret := C.tiledb_array_schema_get_capacity(a.context.tiledbContext, a.tiledbArraySchema, &capacity)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting capacity for tiledb arraySchema: %s", a.context.LastError())
	}
	return uint64(capacity), nil
}

// SetCellOrder set the cell order
func (a *ArraySchema) SetCellOrder(cellOrder Layout) error {
	ret := C.tiledb_array_schema_set_cell_order(a.context.tiledbContext, a.tiledbArraySchema, C.tiledb_layout_t(cellOrder))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting cell order for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// CellOrder return the cell order
func (a *ArraySchema) CellOrder() (Layout, error) {
	var cellOrder C.tiledb_layout_t
	ret := C.tiledb_array_schema_get_cell_order(a.context.tiledbContext, a.tiledbArraySchema, &cellOrder)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting cell order for tiledb arraySchema: %s", a.context.LastError())
	}
	return Layout(cellOrder), nil
}

// SetTileOrder set the tile order
func (a *ArraySchema) SetTileOrder(tileOrder Layout) error {
	ret := C.tiledb_array_schema_set_tile_order(a.context.tiledbContext, a.tiledbArraySchema, C.tiledb_layout_t(tileOrder))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting cell order for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// TileOrder return the tile order
func (a *ArraySchema) TileOrder() (Layout, error) {
	var cellOrder C.tiledb_layout_t
	ret := C.tiledb_array_schema_get_tile_order(a.context.tiledbContext, a.tiledbArraySchema, &cellOrder)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("Error getting cell order for tiledb arraySchema: %s", a.context.LastError())
	}
	return Layout(cellOrder), nil
}

// SetCoordsFilterList sets the filter list used for coordinates
func (a *ArraySchema) SetCoordsFilterList(filterList *FilterList) error {
	ret := C.tiledb_array_schema_set_coords_filter_list(a.context.tiledbContext, a.tiledbArraySchema, filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting coordinates filter list for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// CoordsFilterList Returns a copy of the filter list of the coordinates.
func (a *ArraySchema) CoordsFilterList() (*FilterList, error) {
	filterList := FilterList{context: a.context}
	ret := C.tiledb_array_schema_get_coords_filter_list(a.context.tiledbContext, a.tiledbArraySchema, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting coordinates filter list for tiledb arraySchema: %s", a.context.LastError())
	}
	return &filterList, nil
}

// SetOffsetsFilterList sets the filter list for the offsets of
// variable-length attributes
func (a *ArraySchema) SetOffsetsFilterList(filterList *FilterList) error {
	ret := C.tiledb_array_schema_set_offsets_filter_list(a.context.tiledbContext, a.tiledbArraySchema, filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting offsets filter list for tiledb arraySchema: %s", a.context.LastError())
	}
	return nil
}

// OffsetsFilterList returns a copy of the FilterList of the offsets for
// variable-length attributes.
func (a *ArraySchema) OffsetsFilterList() (*FilterList, error) {
	filterList := FilterList{context: a.context}
	ret := C.tiledb_array_schema_get_offsets_filter_list(a.context.tiledbContext, a.tiledbArraySchema, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting offsets filter list for tiledb arraySchema: %s", a.context.LastError())
	}
	return &filterList, nil
}

// Check validates the schema
func (a *ArraySchema) Check() error {
	ret := C.tiledb_array_schema_check(a.context.tiledbContext, a.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in checking arraySchema: %s", a.context.LastError())
	}
	return nil
}

// LoadArraySchema reads a directory for a ArraySchema
func LoadArraySchema(context *Context, path string) (*ArraySchema, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	a := ArraySchema{context: context}
	ret := C.tiledb_array_schema_load(a.context.tiledbContext, cpath, &a.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in loading arraySchema from %s: %s", path, a.context.LastError())
	}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&a, func(arraySchema *ArraySchema) {
		arraySchema.Free()
	})
	return &a, nil
}

// LoadArraySchemaWithKey retrieves the schema of an encrypted array from the disk, creating an array schema struct.
func LoadArraySchemaWithKey(context *Context, path string, encryptionType EncryptionType, key string) (*ArraySchema, error) {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	a := ArraySchema{context: context}
	ret := C.tiledb_array_schema_load_with_key(a.context.tiledbContext, cpath, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), &a.tiledbArraySchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in loading arraySchema with key from %s: %s", path, a.context.LastError())
	}
	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&a, func(arraySchema *ArraySchema) {
		arraySchema.Free()
	})
	return &a, nil
}

// DumpSTDOUT Dumps the array schema in ASCII format to stdout
func (a *ArraySchema) DumpSTDOUT() error {
	ret := C.tiledb_array_schema_dump(a.context.tiledbContext, a.tiledbArraySchema, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping array schema to stdout: %s", a.context.LastError())
	}
	return nil
}

// Dump Dumps the array schema in ASCII format in the selected output.
func (a *ArraySchema) Dump(path string) error {

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

	// Dump array schema to file
	ret := C.tiledb_array_schema_dump(a.context.tiledbContext, a.tiledbArraySchema, cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping array schema to file %s: %s", path, a.context.LastError())
	}
	return nil
}

// Type fetch the tiledb array type
func (a *ArraySchema) Type() (ArrayType, error) {

	var array_type C.tiledb_array_type_t
	ret := C.tiledb_array_schema_get_array_type(a.context.tiledbContext, a.tiledbArraySchema, &array_type)
	if ret != C.TILEDB_OK {
		return TILEDB_DENSE, fmt.Errorf("Error fetching array schema type: %s", a.context.LastError())
	}

	return ArrayType(array_type), nil
}
