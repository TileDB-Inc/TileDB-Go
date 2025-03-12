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

type dimensionLabelHandle struct{ *capiHandle }

func freeCapiDimensionLabel(c unsafe.Pointer) {
	C.tiledb_dimension_label_free((**C.tiledb_dimension_label_t)(unsafe.Pointer(&c)))
}

func newDimensionLabelHandle(ptr *C.tiledb_dimension_label_t) dimensionLabelHandle {
	return dimensionLabelHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiDimensionLabel)}
}

func (x dimensionLabelHandle) Get() *C.tiledb_dimension_label_t {
	return (*C.tiledb_dimension_label_t)(x.capiHandle.Get())
}

type DimensionLabel struct {
	tiledbDimensionLabel dimensionLabelHandle
	context              *Context
}

func newDimensionLabelFromHandle(context *Context, handle dimensionLabelHandle) *DimensionLabel {
	return &DimensionLabel{tiledbDimensionLabel: handle, context: context}
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (d *DimensionLabel) Free() {
	d.tiledbDimensionLabel.Free()
}

// DimensionIndex returns the index of the dimension the dimension label provides labels for.
func (d *DimensionLabel) DimensionIndex() (uint32, error) {
	var dimensionIndex C.uint32_t
	ret := C.tiledb_dimension_label_get_dimension_index(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &dimensionIndex)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error fetching dimension index for dimension label: %w", d.context.LastError())
	}

	return uint32(dimensionIndex), nil
}

// AttributeName returns the name of the attribute the label data is stored under.
func (d *DimensionLabel) AttributeName() (string, error) {
	var cLabelAttrName *C.char // d must be kept alive while cLabelAttrName is being accessed.
	ret := C.tiledb_dimension_label_get_label_attr_name(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &cLabelAttrName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting dimension label attribute name: %w", d.context.LastError())
	}

	name := C.GoString(cLabelAttrName) // copies cLabelAttrName
	runtime.KeepAlive(d)
	return name, nil
}

// CellValNum returns the number of values per cell for the labels on the dimension label.
// For variable-sized labels the result is TILEDB_VAR_NUM.
func (d *DimensionLabel) CellValNum() (uint32, error) {
	var labelCellValNum C.uint32_t
	ret := C.tiledb_dimension_label_get_label_cell_val_num(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &labelCellValNum)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error fetching cell val num for dimension label: %w", d.context.LastError())
	}

	return uint32(labelCellValNum), nil
}

// Order returns the order of the labels on the dimension label.
func (d *DimensionLabel) Order() (DataOrder, error) {
	var labelOrder C.tiledb_data_order_t
	ret := C.tiledb_dimension_label_get_label_order(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &labelOrder)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error fetching label order for dimension label: %w", d.context.LastError())
	}

	return DataOrder(labelOrder), nil
}

// Type returns the underlying Datatype for the dimension label.
func (d *DimensionLabel) Type() (Datatype, error) {
	var dataType C.tiledb_datatype_t
	ret := C.tiledb_dimension_label_get_label_type(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &dataType)
	runtime.KeepAlive(d)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error fetching dimension label type: %w", d.context.LastError())
	}

	return Datatype(dataType), nil
}

// Name returns the name for the dimension label.
func (d *DimensionLabel) Name() (string, error) {
	var cLabelName *C.char // d must be kept alive while cLabelName is being accessed.
	ret := C.tiledb_dimension_label_get_name(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &cLabelName)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting dimension label name: %w", d.context.LastError())
	}

	labelName := C.GoString(cLabelName) // copies cLabelName
	runtime.KeepAlive(d)
	return labelName, nil
}

// Uri Returns the Uri for the dimension label array.
func (d *DimensionLabel) URI() (string, error) {
	var cLabelUri *C.char // d must be kept alive while cLabelUri is being accessed.
	ret := C.tiledb_dimension_label_get_uri(d.context.tiledbContext.Get(), d.tiledbDimensionLabel.Get(), &cLabelUri)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting dimension label URI: %w", d.context.LastError())
	}

	labelUri := C.GoString(cLabelUri) // copies cLabelUri
	runtime.KeepAlive(d)
	return labelUri, nil
}

// AddDimensionLabel adds a dimension label to the array schema.
func (a *ArraySchema) AddDimensionLabel(dimIndex uint32, name string, order DataOrder, labelType Datatype) error {
	cLabelName := C.CString(name)
	ret := C.tiledb_array_schema_add_dimension_label(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		C.uint32_t(dimIndex), cLabelName, C.tiledb_data_order_t(order), C.tiledb_datatype_t(labelType))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding dimension label to ArraySchema: %w", a.context.LastError())
	}

	return nil
}

// DimensionLabelFromName retrieves a dimension label from an array schema with the requested index.
func (a *ArraySchema) DimensionLabelFromIndex(labelIdx uint64) (*DimensionLabel, error) {
	var dimLabelPtr *C.tiledb_dimension_label_t
	ret := C.tiledb_array_schema_get_dimension_label_from_index(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		C.uint64_t(labelIdx), &dimLabelPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting dimension label '%d' for ArraySchema: %w", labelIdx, a.context.LastError())
	}

	return newDimensionLabelFromHandle(a.context, newDimensionLabelHandle(dimLabelPtr)), nil
}

// DimensionLabelFromName retrieves a dimension label from an array schema with the requested name.
func (a *ArraySchema) DimensionLabelFromName(name string) (*DimensionLabel, error) {
	cAttrName := C.CString(name)
	defer C.free(unsafe.Pointer(cAttrName))
	var dimLabelPtr *C.tiledb_dimension_label_t
	ret := C.tiledb_array_schema_get_dimension_label_from_name(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		cAttrName, &dimLabelPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting dimension label '%s' for ArraySchema: %w", name, a.context.LastError())
	}

	return newDimensionLabelFromHandle(a.context, newDimensionLabelHandle(dimLabelPtr)), nil
}

// HasDimensionLabel checks whether the array schema has a dimension label of the given name.
func (a *ArraySchema) HasDimensionLabel(name string) (bool, error) {
	cLabelName := C.CString(name)
	defer C.free(unsafe.Pointer(cLabelName))

	var hasLabel C.int32_t
	ret := C.tiledb_array_schema_has_dimension_label(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		cLabelName, &hasLabel)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error checking ArraySchema for dimension label '%s': %w", name, a.context.LastError())
	}

	return hasLabel != 0, nil
}

// DimensionLabelsNum returns the number of dimension label in this array schema
func (a *ArraySchema) DimensionLabelsNum() (uint64, error) {
	var labelNum C.uint64_t

	ret := C.tiledb_array_schema_get_dimension_label_num(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(), &labelNum)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error fetching dimension label number: %w", a.context.LastError())
	}

	return uint64(labelNum), nil
}

// SetDimensionLabelFilterList sets a filter on a dimension label filter in an array schema.
func (a *ArraySchema) SetDimensionLabelFilterList(name string, filterList FilterList) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	ret := C.tiledb_array_schema_set_dimension_label_filter_list(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		cName, filterList.tiledbFilterList.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(filterList)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting dimension label filter list on ArraySchema: %w", a.context.LastError())
	}

	return nil
}

// SetDimensionLabelTileExtent sets the tile extent for the dimension label
func (a *ArraySchema) SetDimensionLabelTileExtent(labelName string, dimType Datatype, extent interface{}) error {
	cName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cName))

	extentType := reflect.TypeOf(extent).Kind()
	if extentType != dimType.ReflectKind() {
		return fmt.Errorf("dimension and extent do not have the same data types. Dimension: %s, Extent: %s",
			dimType.ReflectKind(), extentType)
	}

	// Create extent void*
	var cExtent unsafe.Pointer
	switch tmpExtent := extent.(type) {
	case int8:
		cExtent = unsafe.Pointer(&tmpExtent)
	case int16:
		cExtent = unsafe.Pointer(&tmpExtent)
	case int32:
		cExtent = unsafe.Pointer(&tmpExtent)
	case int64:
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint8:
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint16:
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint32:
		cExtent = unsafe.Pointer(&tmpExtent)
	case uint64:
		cExtent = unsafe.Pointer(&tmpExtent)
	case float32:
		cExtent = unsafe.Pointer(&tmpExtent)
	case float64:
		cExtent = unsafe.Pointer(&tmpExtent)
	case bool:
		cExtent = unsafe.Pointer(&tmpExtent)
	default:
		return fmt.Errorf("unrecognized dimension datatype passed to SetDimensionLabelTileExtent: %s",
			dimType.String())
	}

	ret := C.tiledb_array_schema_set_dimension_label_tile_extent(a.context.tiledbContext.Get(), a.tiledbArraySchema.Get(),
		cName, C.tiledb_datatype_t(dimType), cExtent)
	runtime.KeepAlive(a)
	// cExtent is being kept alive by passing it to cgo call.
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting dimension label tile extent on ArraySchema: %w", a.context.LastError())
	}

	return nil
}

// getDimensionLabelDataType retrieves a dimension label Datatype from the schema using experimental APIs.
func (q *Query) getDimensionLabelDataType(labelName string) (Datatype, error) {
	schema, err := q.array.Schema()
	if err != nil {
		return 0, fmt.Errorf("could not get schema for getDimensionLabelDatatype: %w", err)
	}
	defer schema.Free()

	dimLabel, err := schema.DimensionLabelFromName(labelName)
	if err != nil {
		return 0, fmt.Errorf("could not get dimension label %s for getDimensionLabelDatatype: %w", labelName, err)
	}
	defer dimLabel.Free()

	datatype, err := dimLabel.Type()
	if err != nil {
		return 0, fmt.Errorf("could not get dimension label type for getDimensionLabelDatatype: %w", err)
	}

	return datatype, nil
}

// GetDimensionLabelRangeNum returns the number of ranges for a dimension label
func (sa *Subarray) GetDimensionLabelRangeNum(labelName string) (uint64, error) {
	var rangeNum C.uint64_t

	cLabelName := C.CString(labelName)
	defer C.free(unsafe.Pointer(cLabelName))

	ret := C.tiledb_subarray_get_label_range_num(sa.context.tiledbContext.Get(), sa.subarray, cLabelName, &rangeNum)
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error retrieving subarray label range num: %w", sa.context.LastError())
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
		ret = C.tiledb_subarray_add_label_range_var(sa.context.tiledbContext.Get(), sa.subarray, cLabelName,
			slicePtr(startSlice), C.uint64_t(len(startSlice)), slicePtr(endSlice), C.uint64_t(len(endSlice)))
	} else {
		startValue := addressableValue(r.start)
		endValue := addressableValue(r.end)
		ret = C.tiledb_subarray_add_label_range(sa.context.tiledbContext.Get(), sa.subarray, cLabelName,
			startValue.UnsafePointer(), endValue.UnsafePointer(), nil)
	}
	runtime.KeepAlive(sa)
	// The start and end values are being kept alive by passing them to cgo call.
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding subarray label range: %w", sa.context.LastError())
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
		ret = C.tiledb_subarray_get_label_range_var_size(sa.context.tiledbContext.Get(), sa.subarray, cLabelName, C.uint64_t(rangeNum),
			(*C.uint64_t)(unsafe.Pointer(&startSize)), (*C.uint64_t)(unsafe.Pointer(&endSize)))
		if ret == C.TILEDB_OK {
			var sp, ep unsafe.Pointer
			var startData, endData []byte
			if startSize > 0 {
				startData = make([]byte, int(startSize))
				sp = slicePtr(startData)
			}
			if endSize > 0 {
				endData = make([]byte, int(endSize))
				ep = slicePtr(endData)
			}
			ret = C.tiledb_subarray_get_label_range_var(sa.context.tiledbContext.Get(), sa.subarray,
				cLabelName, C.uint64_t(rangeNum), sp, ep)
			if ret == C.TILEDB_OK {
				r.start = string(startData)
				r.end = string(endData)
			}
		}
	} else {
		var startPointer, endPointer, stridePointer unsafe.Pointer
		ret = C.tiledb_subarray_get_label_range(sa.context.tiledbContext.Get(), sa.subarray,
			cLabelName, C.uint64_t(rangeNum), &startPointer, &endPointer, &stridePointer)
		typ := dt.ReflectType()
		if ret == C.TILEDB_OK {
			r.start = reflect.NewAt(typ, startPointer).Elem().Interface()
			r.end = reflect.NewAt(typ, endPointer).Elem().Interface()
		}
	}
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("error retrieving subarray range for label %s and range num %d: %w", labelName, rangeNum, sa.context.LastError())
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
