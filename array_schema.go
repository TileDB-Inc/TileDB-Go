package tiledb

/*
#include <tiledb/tiledb.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// arraySchemaHandle safely wraps a pointer to tiledb_array_schema_t.
// This handle type differs from the rest because it supports replacing the
// handle after creation. For this reason, its member functions must use a pointer receiver.
type arraySchemaHandle struct {
	// An unsafe pointer to the capiHandle holding the schema.
	// This pointer must be accessed using atomic functions.
	// We cannot wrap atomic.Pointer[capiHandle] in a struct and pass it around because of lint warnings,
	// and we cannot mutate the native handle of an existing capiHandle because it's impossible to atomically
	// set the finalizer.
	h unsafe.Pointer
}

func freeCapiArraySchema(c unsafe.Pointer) {
	C.tiledb_array_schema_free((**C.tiledb_array_schema_t)(unsafe.Pointer(&c)))
}

func newArraySchemaCapiHandle(ptr *C.tiledb_array_schema_t) *capiHandle {
	return newCapiHandle(unsafe.Pointer(ptr), freeCapiArraySchema)
}

func newArraySchemaHandle(ptr *C.tiledb_array_schema_t) arraySchemaHandle {
	h := newArraySchemaCapiHandle(ptr)
	return arraySchemaHandle{h: unsafe.Pointer(h)}
}

func (x *arraySchemaHandle) loadCapiHandle() *capiHandle {
	return (*capiHandle)(atomic.LoadPointer(&x.h))
}

func (x *arraySchemaHandle) Get() *C.tiledb_array_schema_t {
	return (*C.tiledb_array_schema_t)(x.loadCapiHandle().Get())
}

// Reset reassigns the native handle held by the array schema handle, and frees the existing one.
// This method is safe to call from multiple goroutines.
func (x *arraySchemaHandle) Reset(ptr *C.tiledb_array_schema_t) {
	h := newArraySchemaCapiHandle(ptr)
	prevHandle := atomic.SwapPointer(&x.h, unsafe.Pointer(h))
	// If the handle has been freed previously, this won't fail.
	(*capiHandle)(prevHandle).Free()
}

func (x *arraySchemaHandle) Free() {
	x.loadCapiHandle().Free()
}

/*
ArraySchema describes an array.

The schema is an independent description of an array. A schema can be used to create multiple array’s, and stores information about its domain, cell types, and compression details. An array schema is composed of:

	A Domain
	A set of Attributes
	Memory layout definitions: tile and cell
	Compression details for Array level factors like offsets and coordinates
*/
type ArraySchema struct {
	tiledbArraySchema arraySchemaHandle
	context           *Context
}

func newArraySchemaFromHandle(context *Context, handle arraySchemaHandle) *ArraySchema {
	return &ArraySchema{tiledbArraySchema: handle, context: context}
}

// MarshalJSON marshals arraySchema struct to json using tiledb.
func (a *ArraySchema) MarshalJSON() ([]byte, error) {
	bs, err := SerializeArraySchema(a, TILEDB_JSON, false)
	if err != nil {
		return nil, fmt.Errorf("error marshaling json for array schema: %w", a.context.LastError())
	}
	return bs, nil
}

// Context exposes the internal TileDB context used to initialize the array schema.
func (a *ArraySchema) Context() *Context {
	return a.context
}

// UnmarshalJSON marshals arraySchema struct to json using tiledb.
func (a *ArraySchema) UnmarshalJSON(b []byte) error {
	var err error
	if a.context == nil {
		a.context, err = NewContext(nil)
		if err != nil {
			return err
		}
	}

	// tiledb c expect the byte array to include the null terminator
	bytesWithNullTerminator := b
	size := len(b)
	// Add the null terminator if it is missing
	if b[size-1] != 0 {
		// If we need to add the null terminator we must first create a copy of the
		// byte array, the marshaler does not allow editing the input byte array
		bytesWithNullTerminator = make([]byte, size+1)
		copy(bytesWithNullTerminator, b)
		bytesWithNullTerminator[size] = 0
	}

	// Wrap the input byte slice in a Buffer (does not copy)
	buffer, err := NewBuffer(a.context)
	if err != nil {
		return fmt.Errorf("error unmarshaling json for array schema: %w", a.context.LastError())
	}
	defer buffer.Free()
	err = buffer.SetBuffer(bytesWithNullTerminator)
	if err != nil {
		return fmt.Errorf("error unmarshaling json for array schema: %w", a.context.LastError())
	}

	// Deserialize into a new array schema
	var newCSchema *C.tiledb_array_schema_t
	var cClientSide = C.int32_t(0) // Currently this parameter is unused in libtiledb
	ret := C.tiledb_deserialize_array_schema(a.context.tiledbContext.Get(), buffer.tiledbBuffer.Get(), C.TILEDB_JSON, cClientSide, &newCSchema)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deserializing array schema: %w", a.context.LastError())
	}

	// Replace the C schema object with the deserialized one.
	a.tiledbArraySchema.Reset(newCSchema)

	return nil
}

// NewArraySchema allocates a new ArraySchema.
func NewArraySchema(tdbCtx *Context, arrayType ArrayType) (*ArraySchema, error) {
	var arraySchemaPtr *C.tiledb_array_schema_t
	ret := C.tiledb_array_schema_alloc(tdbCtx.tiledbContext.Get(), C.tiledb_array_type_t(arrayType), &arraySchemaPtr)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb arraySchema: %w", tdbCtx.LastError())
	}
	return newArraySchemaFromHandle(tdbCtx, newArraySchemaHandle(arraySchemaPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (a *ArraySchema) Free() {
	a.tiledbArraySchema.Free()
}

// AddAttributes adds one or more attributes to the array.
func (a *ArraySchema) AddAttributes(attributes ...*Attribute) error {
	for _, attribute := range attributes {
		ret := C.tiledb_array_schema_add_attribute(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), attribute.tiledbAttribute.Get())
		runtime.KeepAlive(a)
		runtime.KeepAlive(attribute)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error adding attributes to tiledb arraySchema: %w", a.context.LastError())
		}
	}
	return nil
}

// AttributeNum returns the number of attributes.
func (a *ArraySchema) AttributeNum() (uint, error) {
	var attrNum C.uint32_t
	ret := C.tiledb_array_schema_get_attribute_num(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &attrNum)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting attribute number for tiledb arraySchema: %w", a.context.LastError())
	}
	return uint(attrNum), nil
}

// AttributeFromIndex gets a copy of an Attribute in the schema by name.
func (a *ArraySchema) AttributeFromIndex(index uint) (*Attribute, error) {
	var attributePtr *C.tiledb_attribute_t
	ret := C.tiledb_array_schema_get_attribute_from_index(
		a.context.tiledbContext.Get(),
		a.tiledbArraySchema.Get(),
		C.uint32_t(index),
		&attributePtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting attribute %d for tiledb arraySchema: %w", index, a.context.LastError())
	}
	return newAttributeFromHandle(a.context, newAttributeHandle(attributePtr)), nil
}

// AttributeFromName gets a copy of an Attribute in the schema by index.
// Attributes are ordered the same way they were defined when
// constructing the array schema.
func (a *ArraySchema) AttributeFromName(attrName string) (*Attribute, error) {
	cAttrName := C.CString(attrName)
	defer C.free(unsafe.Pointer(cAttrName))
	var attributePtr *C.tiledb_attribute_t
	ret := C.tiledb_array_schema_get_attribute_from_name(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), cAttrName, &attributePtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting attribute %s for tiledb arraySchema: %w", attrName, a.context.LastError())
	}
	return newAttributeFromHandle(a.context, newAttributeHandle(attributePtr)), nil
}

// HasAttribute returns true if attribute: `attrName` is part of the schema.
func (a *ArraySchema) HasAttribute(attrName string) (bool, error) {
	var hasAttr C.int32_t
	cAttrName := C.CString(attrName)
	defer C.free(unsafe.Pointer(cAttrName))
	ret := C.tiledb_array_schema_has_attribute(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), cAttrName, &hasAttr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error finding attribute %s in schema: %w", attrName, a.context.LastError())
	}

	if hasAttr == 0 {
		return false, nil
	}

	return true, nil
}

// SetAllowsDups sets whether the array can allow coordinate duplicates or not.
// Applicable only to sparse arrays (it errors out if set to `1` for dense
// arrays).
func (a *ArraySchema) SetAllowsDups(allowsDups bool) error {
	allowsDupsInt := 0
	if allowsDups {
		allowsDupsInt = 1
	}

	ret := C.tiledb_array_schema_set_allows_dups(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), C.int32_t(allowsDupsInt))
	runtime.KeepAlive(a)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting allows dups for schema: %w", a.context.LastError())
	}

	return nil
}

// AllowsDups gets whether the array can allow coordinate duplicates or not.
// It should always be `0` for dense arrays.
func (a *ArraySchema) AllowsDups() (bool, error) {
	var allowsDups C.int32_t
	ret := C.tiledb_array_schema_get_allows_dups(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &allowsDups)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error getting allows dups for schema: %w", a.context.LastError())
	}

	if allowsDups == 0 {
		return false, nil
	}

	return true, nil
}

// Attributes gets all attributes in the array.
func (a *ArraySchema) Attributes() ([]*Attribute, error) {
	attributes := make([]*Attribute, 0)

	attrNum, err := a.AttributeNum()
	if err != nil {
		return nil, fmt.Errorf("error getting AttributeNum: %w", err)
	}

	for i := uint(0); i < attrNum; i++ {
		attribute, err := a.AttributeFromIndex(i)
		if err != nil {
			return nil, fmt.Errorf("error getting Attribute: %w", err)
		}
		attributes = append(attributes, attribute)
	}
	return attributes, nil
}

// SetDomain sets the array domain.
func (a *ArraySchema) SetDomain(domain *Domain) error {
	ret := C.tiledb_array_schema_set_domain(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), domain.tiledbDomain.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(domain)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting domain for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// Domain returns the array's domain.
func (a *ArraySchema) Domain() (*Domain, error) {
	var domainPtr *C.tiledb_domain_t
	ret := C.tiledb_array_schema_get_domain(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &domainPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting domain for tiledb arraySchema: %w", a.context.LastError())
	}

	return newDomainFromHandle(a.context, newDomainHandle(domainPtr)), nil
}

// SetCapacity sets the tile capacity.
func (a *ArraySchema) SetCapacity(capacity uint64) error {
	ret := C.tiledb_array_schema_set_capacity(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), C.uint64_t(capacity))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting capacity for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// Capacity returns the tile capacity.
func (a *ArraySchema) Capacity() (uint64, error) {
	var capacity C.uint64_t
	ret := C.tiledb_array_schema_get_capacity(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &capacity)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting capacity for tiledb arraySchema: %w", a.context.LastError())
	}
	return uint64(capacity), nil
}

// SetCellOrder sets the cell order.
func (a *ArraySchema) SetCellOrder(cellOrder Layout) error {
	ret := C.tiledb_array_schema_set_cell_order(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), C.tiledb_layout_t(cellOrder))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting cell order for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// CellOrder returns the cell order.
func (a *ArraySchema) CellOrder() (Layout, error) {
	var cellOrder C.tiledb_layout_t
	ret := C.tiledb_array_schema_get_cell_order(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &cellOrder)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting cell order for tiledb arraySchema: %w", a.context.LastError())
	}
	return Layout(cellOrder), nil
}

// SetTileOrder sets the tile order.
func (a *ArraySchema) SetTileOrder(tileOrder Layout) error {
	ret := C.tiledb_array_schema_set_tile_order(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), C.tiledb_layout_t(tileOrder))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting cell order for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// TileOrder returns the tile order.
func (a *ArraySchema) TileOrder() (Layout, error) {
	var cellOrder C.tiledb_layout_t
	ret := C.tiledb_array_schema_get_tile_order(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &cellOrder)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting cell order for tiledb arraySchema: %w", a.context.LastError())
	}
	return Layout(cellOrder), nil
}

// SetCoordsFilterList sets the filter list used for coordinates.
func (a *ArraySchema) SetCoordsFilterList(filterList *FilterList) error {
	ret := C.tiledb_array_schema_set_coords_filter_list(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), filterList.tiledbFilterList.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(filterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting coordinates filter list for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// CoordsFilterList returns a copy of the filter list of the coordinates.
func (a *ArraySchema) CoordsFilterList() (*FilterList, error) {
	var filterListPtr *C.tiledb_filter_list_t
	ret := C.tiledb_array_schema_get_coords_filter_list(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &filterListPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting coordinates filter list for tiledb arraySchema: %w", a.context.LastError())
	}

	return newFilterListFromHandle(a.context, newFilterListHandle(filterListPtr)), nil
}

// SetOffsetsFilterList sets the filter list for the offsets of
// variable-length attributes.
func (a *ArraySchema) SetOffsetsFilterList(filterList *FilterList) error {
	runtime.KeepAlive(a)
	runtime.KeepAlive(filterList)
	ret := C.tiledb_array_schema_set_offsets_filter_list(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), filterList.tiledbFilterList.Get())
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting offsets filter list for tiledb arraySchema: %w", a.context.LastError())
	}
	return nil
}

// OffsetsFilterList returns a copy of the FilterList of the offsets for
// variable-length attributes.
func (a *ArraySchema) OffsetsFilterList() (*FilterList, error) {
	var filterListPtr *C.tiledb_filter_list_t
	ret := C.tiledb_array_schema_get_offsets_filter_list(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &filterListPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting offsets filter list for tiledb arraySchema: %w", a.context.LastError())
	}

	return newFilterListFromHandle(a.context, newFilterListHandle(filterListPtr)), nil
}

// Check validates the schema.
func (a *ArraySchema) Check() error {
	ret := C.tiledb_array_schema_check(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error in checking arraySchema: %w", a.context.LastError())
	}
	return nil
}

// LoadArraySchema reads a directory for an ArraySchema.
func LoadArraySchema(context *Context, path string) (*ArraySchema, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var arraySchemaPtr *C.tiledb_array_schema_t
	ret := C.tiledb_array_schema_load(context.tiledbContext.Get(), cpath, &arraySchemaPtr)
	runtime.KeepAlive(context)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in loading arraySchema from %s: %w", path, context.LastError())
	}
	return newArraySchemaFromHandle(context, newArraySchemaHandle(arraySchemaPtr)), nil
}

// DumpToString returns the array schema in ASCII format as a string.
func (a *ArraySchema) DumpToString() (string, error) {
	var cStr *C.tiledb_string_t
	ret := C.tiledb_array_schema_dump_str(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &cStr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error dumping array schema to string: %w", a.context.LastError())
	}
	defer C.tiledb_string_free(&cStr)

	goStr, err := stringHandleToString(cStr)
	if err != nil {
		return "", fmt.Errorf("error converting array schema dump to string: %w", err)
	}
	return goStr, nil
}

func (a *ArraySchema) DumpSTDOUT() error {
	goStr, err := a.DumpToString()
	if err != nil {
		return err
	}
	fmt.Print(goStr)
	return nil
}

func (a *ArraySchema) Dump(path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}
	goStr, err := a.DumpToString()
	if err != nil {
		return err
	}
	err = os.WriteFile(path, []byte(goStr), 0644)
	if err != nil {
		return fmt.Errorf("error writing array schema dump to file %s: %w", path, err)
	}
	return nil
}

// Type fetches the tiledb array type.
func (a *ArraySchema) Type() (ArrayType, error) {
	var arrayType C.tiledb_array_type_t
	ret := C.tiledb_array_schema_get_array_type(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &arrayType)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return TILEDB_DENSE, fmt.Errorf("error fetching array schema type: %w", a.context.LastError())
	}

	return ArrayType(arrayType), nil
}
