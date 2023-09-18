//go:build experimental

// This file declares Go bindings for experimental features in TileDB.
// Experimental APIs to do not fall under the API compatibility guarantees and
// might change between TileDB versions

package tiledb

/*
#cgo LDFLAGS: -ltiledb
#cgo linux LDFLAGS: -ldl
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"reflect"
	"unsafe"
)

type DimensionLabel struct {
	tiledbDimensionLabel *C.tiledb_dimension_label_t
	context              *Context
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (d *DimensionLabel) Free() {
	if d.tiledbDimensionLabel != nil {
		C.tiledb_dimension_label_free(&d.tiledbDimensionLabel)
	}
}

// DimensionIndex returns the index of the dimension the dimension label provides labels for.
func (d *DimensionLabel) DimensionIndex() (uint32, error) {
	var dimensionIndex C.uint32_t
	ret := C.tiledb_dimension_label_get_dimension_index(d.context.tiledbContext, d.tiledbDimensionLabel, &dimensionIndex)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension index for dimension label: %s", d.context.LastError())
	}

	return uint32(dimensionIndex), nil
}

// AttributeName returns the name of the attribute the label data is stored under.
func (d *DimensionLabel) AttributeName() (string, error) {
	var labelAttrName *C.char
	ret := C.tiledb_dimension_label_get_label_attr_name(d.context.tiledbContext, d.tiledbDimensionLabel, &labelAttrName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label attribute name: %s", d.context.LastError())
	}

	return C.GoString(labelAttrName), nil
}

// CellValNum returns the number of values per cell for the labels on the dimension label.
// For variable-sized labels the result is TILEDB_VAR_NUM.
func (d *DimensionLabel) CellValNum() (uint32, error) {
	var labelCellValNum C.uint32_t
	ret := C.tiledb_dimension_label_get_label_cell_val_num(d.context.tiledbContext, d.tiledbDimensionLabel, &labelCellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching cell val num for dimension label: %s", d.context.LastError())
	}

	return uint32(labelCellValNum), nil
}

// Order returns the order of the labels on the dimension label.
func (d *DimensionLabel) Order() (DataOrder, error) {
	var labelOrder C.tiledb_data_order_t
	ret := C.tiledb_dimension_label_get_label_order(d.context.tiledbContext, d.tiledbDimensionLabel, &labelOrder)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching label order for dimension label: %s", d.context.LastError())
	}

	return DataOrder(labelOrder), nil
}

// Type returns the underlying Datatype for the dimension label.
func (d *DimensionLabel) Type() (Datatype, error) {
	var dataType C.tiledb_datatype_t
	ret := C.tiledb_dimension_label_get_label_type(d.context.tiledbContext, d.tiledbDimensionLabel, &dataType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension label type: %s", d.context.LastError())
	}

	return Datatype(dataType), nil
}

// Name returns the name for the dimension label.
func (d *DimensionLabel) Name() (string, error) {
	var labelName *C.char
	ret := C.tiledb_dimension_label_get_name(d.context.tiledbContext, d.tiledbDimensionLabel, &labelName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label name: %s", d.context.LastError())
	}

	return C.GoString(labelName), nil
}

// Uri Returns the Uri for the dimension label array.
func (d *DimensionLabel) URI() (string, error) {
	var labelUri *C.char
	ret := C.tiledb_dimension_label_get_uri(d.context.tiledbContext, d.tiledbDimensionLabel, &labelUri)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label URI: %s", d.context.LastError())
	}

	return C.GoString(labelUri), nil
}

// AddDimensionLabel adds a dimension label to the array schema.
func (a *ArraySchema) AddDimensionLabel(dimIndex uint32, name string, order DataOrder, labelType Datatype) error {
	cLabelName := C.CString(name)
	ret := C.tiledb_array_schema_add_dimension_label(a.context.tiledbContext, a.tiledbArraySchema,
		C.uint32_t(dimIndex), cLabelName, C.tiledb_data_order_t(order), C.tiledb_datatype_t(labelType))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding dimension label to ArraySchema: %s", a.context.LastError())
	}

	return nil
}

// DimensionLabelFromName retrieves a dimension label from an array schema with the requested index.
func (a *ArraySchema) DimensionLabelFromIndex(labelIdx uint64) (*DimensionLabel, error) {
	dimLabel := DimensionLabel{context: a.context}
	ret := C.tiledb_array_schema_get_dimension_label_from_index(a.context.tiledbContext, a.tiledbArraySchema,
		C.uint64_t(labelIdx), &dimLabel.tiledbDimensionLabel)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting dimension label '%d' for ArraySchema: %s", labelIdx, a.context.LastError())
	}

	freeOnGC(&dimLabel)
	return &dimLabel, nil
}

// DimensionLabelFromName retrieves a dimension label from an array schema with the requested name.
func (a *ArraySchema) DimensionLabelFromName(name string) (*DimensionLabel, error) {
	cAttrName := C.CString(name)
	defer C.free(unsafe.Pointer(cAttrName))
	dimLabel := DimensionLabel{context: a.context}
	ret := C.tiledb_array_schema_get_dimension_label_from_name(a.context.tiledbContext, a.tiledbArraySchema,
		cAttrName, &dimLabel.tiledbDimensionLabel)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting dimension label '%s' for ArraySchema: %s", name, a.context.LastError())
	}

	freeOnGC(&dimLabel)
	return &dimLabel, nil
}

// HasDimensionLabel checks whether the array schema has a dimension label of the given name.
func (a *ArraySchema) HasDimensionLabel(name string) (bool, error) {
	cLabelName := C.CString(name)
	defer C.free(unsafe.Pointer(cLabelName))

	var hasLabel C.int32_t
	ret := C.tiledb_array_schema_has_dimension_label(a.context.tiledbContext, a.tiledbArraySchema,
		cLabelName, &hasLabel)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error checking ArraySchema for dimension label '%s': %s", name, a.context.LastError())
	}

	return hasLabel != 0, nil
}

// DimensionLabelsNum returns the number of dimension label in this array schema
func (a *ArraySchema) DimensionLabelsNum() (uint64, error) {
	var labelNum uint64

	ret := C.tiledb_array_schema_get_dimension_label_num(a.context.tiledbContext, a.tiledbArraySchema, (*C.uint64_t)(unsafe.Pointer(&labelNum)))
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension label number: %s", a.context.LastError())
	}

	return labelNum, nil
}

// SetDimensionLabelFilterList sets a filter on a dimension label filter in an array schema.
func (a *ArraySchema) SetDimensionLabelFilterList(name string, filterList FilterList) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_array_schema_set_dimension_label_filter_list(a.context.tiledbContext, a.tiledbArraySchema,
		cName, filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting dimension label filter list on ArraySchema: %s", a.context.LastError())
	}

	return nil
}

// SetDimensionLabelTileExtent sets the tile extent for the dimension label
func (a *ArraySchema) SetDimensionLabelTileExtent(labelName string, dimType Datatype, extent interface{}) error {
	cName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cName))

	extentType := reflect.TypeOf(extent).Kind()
	if extentType != dimType.ReflectKind() {
		return fmt.Errorf("Dimension and extent do not have the same data types. Dimension: %s, Extent: %s",
			dimType.ReflectKind(), extentType)
	}

	// Create extent void*
	var cExtent unsafe.Pointer
	switch dimType {
	case TILEDB_INT8:
		tmpExtent := extent.(int8)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT16:
		tmpExtent := extent.(int16)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT32:
		tmpExtent := extent.(int32)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		tmpExtent := extent.(int64)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT8:
		tmpExtent := extent.(uint8)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT16:
		tmpExtent := extent.(uint16)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT32:
		tmpExtent := extent.(uint32)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_UINT64:
		tmpExtent := extent.(uint64)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT32:
		tmpExtent := extent.(float32)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_FLOAT64:
		tmpExtent := extent.(float64)
		cExtent = unsafe.Pointer(&tmpExtent)
	case TILEDB_BOOL:
		tmpExtent := extent.(bool)
		cExtent = unsafe.Pointer(&tmpExtent)
	default:
		return fmt.Errorf("Unrecognized dimension datatype passed to SetDimensionLabelTileExtent: %s",
			dimType.String())
	}

	ret := C.tiledb_array_schema_set_dimension_label_tile_extent(a.context.tiledbContext, a.tiledbArraySchema,
		cName, C.tiledb_datatype_t(dimType), cExtent)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting dimension label tile extent on ArraySchema: %s", a.context.LastError())
	}

	return nil
}

// getDimensionLabelDataType Retrieve a dimension label Datatype from the schema using experimental APIs.
func (q *Query) getDimensionLabelDataType(labelName string) (Datatype, error) {
	schema, err := q.array.Schema()
	if err != nil {
		return 0, fmt.Errorf("Could not get schema for getDimensionLabelDatatype: %s", err)
	}

	dimLabel, err := schema.DimensionLabelFromName(labelName)
	if err != nil {
		return 0, fmt.Errorf("Could not get dimension label %s for getDimensionLabelDatatype: %s", labelName, err)
	}

	datatype, err := dimLabel.Type()
	if err != nil {
		return 0, fmt.Errorf("Could not get dimension label type for getDimensionLabelDatatype: %s", err)
	}

	return datatype, nil
}
