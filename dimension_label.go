//go:build experimental
// +build experimental

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

// DimensionIndex Returns the index of the dimension the dimension label provides labels for.
func (d *DimensionLabel) DimensionIndex() (uint32, error) {
	var dimensionIndex C.uint32_t
	ret := C.tiledb_dimension_label_get_dimension_index(
		d.context.tiledbContext,
		d.tiledbDimensionLabel,
		&dimensionIndex)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension index for dimension label: %s", d.context.LastError())
	}

	return uint32(dimensionIndex), nil
}

// LabelAttrName Returns the name of the attribute the label data is stored under.
func (d *DimensionLabel) LabelAttrName() (string, error) {
	var labelAttrName *C.char
	ret := C.tiledb_dimension_label_get_label_attr_name(
		d.context.tiledbContext,
		d.tiledbDimensionLabel,
		&labelAttrName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label attribute name: %s", d.context.LastError())
	}

	return C.GoString(labelAttrName), nil
}

// LabelCellValNum Returns the number of values per cell for the labels on the dimension label.
// For variable-sized labels the result is TILEDB_VAR_NUM.
func (d *DimensionLabel) LabelCellValNum() (uint32, error) {
	var labelCellValNum C.uint32_t
	ret := C.tiledb_dimension_label_get_label_cell_val_num(
		d.context.tiledbContext,
		d.tiledbDimensionLabel,
		&labelCellValNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching cell val num for dimension label: %s", d.context.LastError())
	}

	return uint32(labelCellValNum), nil
}

// LabelOrder Returns the order of the labels on the dimension label.
func (d *DimensionLabel) LabelOrder() (DataOrder, error) {
	var labelOrder C.tiledb_data_order_t
	ret := C.tiledb_dimension_label_get_label_order(d.context.tiledbContext, d.tiledbDimensionLabel, &labelOrder)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching label order for dimension label: %s", d.context.LastError())
	}

	return DataOrder(labelOrder), nil
}

// Type Returns the underlying Datatype for the dimension label.
func (d *DimensionLabel) Type() (Datatype, error) {
	var dataType C.tiledb_datatype_t
	ret := C.tiledb_dimension_label_get_label_type(d.context.tiledbContext, d.tiledbDimensionLabel, &dataType)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension label type: %s", d.context.LastError())
	}

	return Datatype(dataType), nil
}

// Name Returns the name for the dimension label.
func (d *DimensionLabel) Name() (string, error) {
	var labelName *C.char
	ret := C.tiledb_dimension_label_get_name(d.context.tiledbContext, d.tiledbDimensionLabel, &labelName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label name: %s", d.context.LastError())
	}

	return C.GoString(labelName), nil
}

// Uri Returns the Uri for the dimension label array.
func (d *DimensionLabel) Uri() (string, error) {
	var labelUri *C.char
	ret := C.tiledb_dimension_label_get_uri(d.context.tiledbContext, d.tiledbDimensionLabel, &labelUri)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label URI: %s", d.context.LastError())
	}

	return C.GoString(labelUri), nil
}

// Dimension label experimental ArraySchema API.

// AddDimensionLabel Adds a dimension label to an array schema.
func (a *ArraySchema) AddDimensionLabel(dimIndex uint32, name string, order DataOrder, labelType Datatype) error {
	cLabelName := C.CString(name)
	ret := C.tiledb_array_schema_add_dimension_label(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		C.uint32_t(dimIndex),
		cLabelName,
		C.tiledb_data_order_t(order),
		C.tiledb_datatype_t(labelType))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding dimension label to ArraySchema: %s", a.context.LastError())
	}

	return nil
}

// DimensionLabelFromName Retrieves a dimension label from an array schema with the requested name.
func (a *ArraySchema) DimensionLabelFromName(name string) (*DimensionLabel, error) {
	cAttrName := C.CString(name)
	defer C.free(unsafe.Pointer(cAttrName))
	dimLabel := DimensionLabel{context: a.context}
	ret := C.tiledb_array_schema_get_dimension_label_from_name(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		cAttrName,
		&dimLabel.tiledbDimensionLabel)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error getting dimension label '%s' for ArraySchema: %s", name, a.context.LastError())
	}

	freeOnGC(&dimLabel)
	return &dimLabel, nil
}

// HasDimensionLabel Checks whether the array schema has a dimension label of the given name.
func (a *ArraySchema) HasDimensionLabel(name string) (bool, error) {
	var hasLabel C.int32_t
	cLabelName := C.CString(name)
	defer C.free(unsafe.Pointer(cLabelName))
	ret := C.tiledb_array_schema_has_dimension_label(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		cLabelName,
		&hasLabel)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf(
			"Error checking ArraySchema for dimension label '%s': %s",
			name,
			a.context.LastError())
	}

	if hasLabel == 0 {
		return false, nil
	}
	return true, nil
}

// SetDimensionLabelFilterList Sets a filter on a dimension label filter in an array schema.
func (a *ArraySchema) SetDimensionLabelFilterList(name string, filterList FilterList) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	ret := C.tiledb_array_schema_set_dimension_label_filter_list(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		cName,
		filterList.tiledbFilterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting dimension label filter list on ArraySchema: %s", a.context.LastError())
	}

	return nil
}

// DimensionLabelNum Retrieves the number of dimension labels attached to an array schema.
func (a *ArraySchema) DimensionLabelNum() (uint64, error) {
	var dimLabelNum C.uint64_t
	ret := C.tiledb_array_schema_get_dimension_label_num(a.context.tiledbContext, a.tiledbArraySchema, &dimLabelNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting dimension label number for ArraySchema: %s", a.context.LastError())
	}

	return uint64(dimLabelNum), nil
}

// DimensionLabelFromIndex Retrieve a dimension label from an array schema by index position.
func (a *ArraySchema) DimensionLabelFromIndex(index uint64) (*DimensionLabel, error) {
	dimLabel := DimensionLabel{context: a.context}
	ret := C.tiledb_array_schema_get_dimension_label_from_index(
		a.context.tiledbContext,
		a.tiledbArraySchema,
		C.uint64_t(index),
		&dimLabel.tiledbDimensionLabel)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf(
			"Error getting dimension label at index %d for ArraySchema: %s",
			index,
			a.context.LastError())
	}

	freeOnGC(&dimLabel)
	return &dimLabel, nil
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
