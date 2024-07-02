package tiledb

/*
#include <tiledb/tiledb_experimental.h>
#include <tiledb/tiledb_serialization.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"reflect"
	"runtime"
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

	return C.GoString(labelAttrName), nil // copies labelAttrName which is memory owned by core
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

	return C.GoString(labelName), nil // copies labelName which is memory owned by core
}

// Uri Returns the Uri for the dimension label array.
func (d *DimensionLabel) URI() (string, error) {
	var labelUri *C.char
	ret := C.tiledb_dimension_label_get_uri(d.context.tiledbContext, d.tiledbDimensionLabel, &labelUri)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting dimension label URI: %s", d.context.LastError())
	}

	return C.GoString(labelUri), nil // copies labelUri which is memory owned by core
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
	var labelNum C.uint64_t

	ret := C.tiledb_array_schema_get_dimension_label_num(a.context.tiledbContext, a.tiledbArraySchema, &labelNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error fetching dimension label number: %s", a.context.LastError())
	}

	return uint64(labelNum), nil
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

	// Use extentPtr to ensure cExtent is not collected before it is passed to tiledb.
	var extentPtr any
	defer runtime.KeepAlive(extentPtr)
	// Create extent void*
	var cExtent unsafe.Pointer
	switch tmpExtent := extent.(type) {
	case int8:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case int16:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case int32:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case int64:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint8:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint16:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint32:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint64:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case float32:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case float64:
		extentPtr = &tmpExtent
		cExtent = unsafe.Pointer(&tmpExtent)
	case bool:
		extentPtr = &tmpExtent
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

// getDimensionLabelDataType retrieves a dimension label Datatype from the schema using experimental APIs.
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

// GetDimensionLabelRangeNum returns the number of ranges for a dimension label
func (sa *Subarray) GetDimensionLabelRangeNum(labelName string) (uint64, error) {
	var rangeNum C.uint64_t

	cLabelName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cLabelName))

	ret := C.tiledb_subarray_get_label_range_num(sa.context.tiledbContext, sa.subarray, cLabelName, &rangeNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error retrieving subarray label range num: %s", sa.context.LastError())
	}

	return uint64(rangeNum), nil
}

// AddDimensionLabelRange adds a range for a dimension label. It checks the types of range and label and
// if the datatype of the range is not the same as the type of the label it returns an error.
func (sa *Subarray) AddDimensionLabelRange(labelName string, r Range) error {
	dt, isVar, err := datatypeOfDimensionLabel(sa.array, labelName)
	if err != nil {
		return err
	}
	if err := r.assertCompatibility(dt, isVar); err != nil {
		return err
	}

	cLabelName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cLabelName))

	var ret C.int32_t
	if isVar {
		startSlice := []byte(r.start.(string))
		endSlice := []byte(r.end.(string))
		ret = C.tiledb_subarray_add_label_range_var(sa.context.tiledbContext, sa.subarray, cLabelName,
			unsafe.Pointer(&startSlice[0]), C.uint64_t(len(startSlice)), unsafe.Pointer(&endSlice[0]), C.uint64_t(len(endSlice)))
		runtime.KeepAlive(startSlice)
		runtime.KeepAlive(endSlice)
	} else {
		startValue := addressableValue(r.start)
		endValue := addressableValue(r.end)
		ret = C.tiledb_subarray_add_label_range(sa.context.tiledbContext, sa.subarray, cLabelName,
			startValue.UnsafePointer(), endValue.UnsafePointer(), nil)
		runtime.KeepAlive(startValue)
		runtime.KeepAlive(endValue)
	}
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding subarray label range: %s", sa.context.LastError())
	}

	return nil
}

// GetDimensionLabelRange retrieves a specific range of the subarray along a given dimension label name.
func (sa *Subarray) GetDimensionLabelRange(labelName string, rangeNum uint64) (Range, error) {
	dt, isVar, err := datatypeOfDimensionLabel(sa.array, labelName)
	if err != nil {
		return Range{}, err
	}

	cLabelName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cLabelName))

	var r Range
	var ret C.int32_t
	if isVar {
		var startSize, endSize uint64
		ret = C.tiledb_subarray_get_label_range_var_size(sa.context.tiledbContext, sa.subarray, cLabelName, C.uint64_t(rangeNum),
			(*C.uint64_t)(unsafe.Pointer(&startSize)), (*C.uint64_t)(unsafe.Pointer(&endSize)))
		if ret == C.TILEDB_OK {
			var sp, ep unsafe.Pointer
			var startData, endData []byte
			if startSize > 0 {
				startData = make([]byte, int(startSize))
				sp = unsafe.Pointer(&startData[0])
			}
			if endSize > 0 {
				endData = make([]byte, int(endSize))
				ep = unsafe.Pointer(&endData[0])
			}
			ret = C.tiledb_subarray_get_label_range_var(sa.context.tiledbContext, sa.subarray,
				cLabelName, C.uint64_t(rangeNum), sp, ep)
			if ret == C.TILEDB_OK {
				r.start = string(startData)
				r.end = string(endData)
			}
		}
	} else {
		var startPointer, endPointer, stridePointer unsafe.Pointer
		ret = C.tiledb_subarray_get_label_range(sa.context.tiledbContext, sa.subarray,
			cLabelName, C.uint64_t(rangeNum), &startPointer, &endPointer, &stridePointer)
		typ := dt.ReflectType()
		if ret == C.TILEDB_OK {
			r.start = reflect.NewAt(typ, startPointer).Elem().Interface()
			r.end = reflect.NewAt(typ, endPointer).Elem().Interface()
		}
	}
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("Error retrieving subarray range for label %s and range num %d: %s", labelName, rangeNum, sa.context.LastError())
	}

	return r, err
}

func datatypeOfDimensionLabel(arr *Array, labelName string) (Datatype, bool, error) {
	schema, err := arr.Schema()
	if err != nil {
		return Datatype(0), false, err
	}

	label, err := schema.DimensionLabelFromName(labelName)
	if err != nil {
		return Datatype(0), false, err
	}

	datatype, err := label.Type()
	if err != nil {
		return Datatype(0), false, err
	}

	cellValNum, err := label.CellValNum()
	if err != nil {
		return Datatype(0), false, err
	}

	return datatype, cellValNum == TILEDB_VAR_NUM, nil
}
