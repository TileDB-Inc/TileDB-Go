package tiledb

/*
#cgo LDFLAGS: -ltiledb
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
KV Schema describing an array.

The schema is an independent description of an array. A schema can be used to create multiple array’s, and stores information about its domain, cell types, and compression details. An array schema is composed of:

    A Domain
    A set of Attributes
    Memory layout definitions: tile and cell
    Compression details for Array level factors like offsets and coordinates
*/
type KVSchema struct {
	tiledbKVSchema *C.tiledb_kv_schema_t
	context        *Context
}

// NewKVSchema alloc a new KVSchema
func NewKVSchema(ctx *Context) (*KVSchema, error) {
	kvSchema := KVSchema{context: ctx}
	ret := C.tiledb_kv_schema_alloc(kvSchema.context.tiledbContext, &kvSchema.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb KVSchema: %s", kvSchema.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&kvSchema, func(kvSchema *KVSchema) {
		kvSchema.Free()
	})

	return &kvSchema, nil
}

// Free tiledb_kv_schema_t that was allocated on heap in c
func (k *KVSchema) Free() {
	if k.tiledbKVSchema != nil {
		C.tiledb_kv_schema_free(&k.tiledbKVSchema)
	}
}

// AddAttributes add one or more attributes to the array
func (k *KVSchema) AddAttributes(attributes ...*Attribute) error {
	for _, attribute := range attributes {
		ret := C.tiledb_kv_schema_add_attribute(k.context.tiledbContext, k.tiledbKVSchema, attribute.tiledbAttribute)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error adding attributes to tiledb KVSchema: %s", k.context.LastError())
		}
	}
	return nil
}

// AttributeNum returns the number of attributes
func (k *KVSchema) AttributeNum() (uint, error) {
	var attrNum C.uint
	ret := C.tiledb_kv_schema_get_attribute_num(k.context.tiledbContext, k.tiledbKVSchema, &attrNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting attribute number for tiledb KVSchema: %s", k.context.LastError())
	}
	return uint(attrNum), nil
}

// AttributeFromIndex get a copy of an Attribute in the schema by name.
func (k *KVSchema) AttributeFromIndex(index uint) (*Attribute, error) {
	attr := Attribute{context: k.context}
	ret := C.tiledb_kv_schema_get_attribute_from_index(k.context.tiledbContext, k.tiledbKVSchema, C.uint(index), &attr.tiledbAttribute)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting attribute %d for tiledb KVSchema: %s", index, k.context.LastError())
	}
	return &attr, nil
}

// AttributeFromName Get a copy of an Attribute in the schema by index.
// Attributes are ordered the same way they were defined when
// constructing the array schemk.
func (k *KVSchema) AttributeFromName(attrName string) (*Attribute, error) {
	cAttrName := C.CString(attrName)
	defer C.free(unsafe.Pointer(cAttrName))
	attr := Attribute{context: k.context}
	ret := C.tiledb_kv_schema_get_attribute_from_name(k.context.tiledbContext, k.tiledbKVSchema, cAttrName, &attr.tiledbAttribute)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting attribute %s for tiledb KVSchema: %s", attrName, k.context.LastError())
	}
	return &attr, nil
}

// Attributes gets all attributes in the array.
func (k *KVSchema) Attributes() ([]*Attribute, error) {
	attributes := make([]*Attribute, 0)

	attrNum, err := k.AttributeNum()
	if err != nil {
		return nil, fmt.Errorf("Error getting AttributeNum: %s", err)
	}

	for i := uint(0); i < attrNum; i++ {
		attribute, err := k.AttributeFromIndex(i)
		if err != nil {
			return nil, fmt.Errorf("Error getting Attribute: %s", err)
		}
		attributes = append(attributes, attribute)
	}
	return attributes, nil
}

// SetCapacity sets the tile capacity.
func (k *KVSchema) SetCapacity(capacity uint64) error {
	ret := C.tiledb_kv_schema_set_capacity(k.context.tiledbContext, k.tiledbKVSchema, C.uint64_t(capacity))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting capacity for tiledb KVSchema: %s", k.context.LastError())
	}
	return nil
}

// Capacity returns the tile capacity.
func (k *KVSchema) Capacity() (uint64, error) {
	var capacity C.uint64_t
	ret := C.tiledb_kv_schema_get_capacity(k.context.tiledbContext, k.tiledbKVSchema, &capacity)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting capacity for tiledb KVSchema: %s", k.context.LastError())
	}
	return uint64(capacity), nil
}

/*
// SetCoordsFilterList sets the filter list used for coordinates
func (k *KVSchema) SetCoordsFilterList(filterList *FilterList) error {
	ret := C.tiledb_kv_schema_set_coords_filter_list(k.context.tiledbContext, k.tiledbKVSchema, filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting coordinates filter list for tiledb KVSchema: %s", k.context.LastError())
	}
	return nil
}

// CoordsFilterList Returns a copy of the filter list of the coordinates.
func (k *KVSchema) CoordsFilterList() (*FilterList, error) {
	filterList := FilterList{context: k.context}
	ret := C.tiledb_kv_schema_get_coords_filter_list(k.context.tiledbContext, k.tiledbKVSchema, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting coordinates filter list for tiledb KVSchema: %s", k.context.LastError())
	}
	return &filterList, nil
}

// SetOffsetsFilterList sets the filter list for the offsets of
// variable-length attributes
func (k *KVSchema) SetOffsetsFilterList(filterList *FilterList) error {
	ret := C.tiledb_kv_schema_set_offsets_filter_list(k.context.tiledbContext, k.tiledbKVSchema, filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting offsets filter list for tiledb KVSchema: %s", k.context.LastError())
	}
	return nil
}

// OffsetsFilterList returns a copy of the FilterList of the offsets for
// variable-length attributes.
func (k *KVSchema) OffsetsFilterList() (*FilterList, error) {
	filterList := FilterList{context: k.context}
	ret := C.tiledb_kv_schema_get_offsets_filter_list(k.context.tiledbContext, k.tiledbKVSchema, &filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting offsets filter list for tiledb KVSchema: %s", k.context.LastError())
	}
	return &filterList, nil
}*/

// Check validates the schema
func (k *KVSchema) Check() error {
	ret := C.tiledb_kv_schema_check(k.context.tiledbContext, k.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error in checking KVSchema: %s", k.context.LastError())
	}
	return nil
}

// LoadKVSchema reads a directory for a KVSchema
func LoadKVSchema(context *Context, path string) (*KVSchema, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	k, err := NewKVSchema(context)
	if err != nil {
		return nil, err
	}
	ret := C.tiledb_kv_schema_load(k.context.tiledbContext, cpath, &k.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in loading KVSchema from %s: %s", path, k.context.LastError())
	}
	return k, nil
}

// LoadKVSchemaWithKey retrieves the schema of an encrypted array from the disk, creating an array schema struct.
func LoadKVSchemaWithKey(context *Context, path string, encryptionType EncryptionType, key string) (*KVSchema, error) {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	k, err := NewKVSchema(context)
	if err != nil {
		return nil, err
	}
	ret := C.tiledb_kv_schema_load_with_key(k.context.tiledbContext, cpath, C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)), &k.tiledbKVSchema)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in loading KVSchema with key from %s: %s", path, k.context.LastError())
	}
	return k, nil
}

// DumpSTDOUT Dumps the array schema in ASCII format to stdout
func (k *KVSchema) DumpSTDOUT() error {
	ret := C.tiledb_kv_schema_dump(k.context.tiledbContext, k.tiledbKVSchema, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping array schema to stdout: %s", k.context.LastError())
	}
	return nil
}

// Dump Dumps the array schema in ASCII format in the selected output.
func (k *KVSchema) Dump(path string) error {

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
	ret := C.tiledb_kv_schema_dump(k.context.tiledbContext, k.tiledbKVSchema, cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping array schema to file %s: %s", path, k.context.LastError())
	}
	return nil
}
