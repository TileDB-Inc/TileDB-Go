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
	"reflect"
	"unsafe"
)

// Subarray is a container of dimension ranges for a tiledb Query.
type Subarray struct {
	array    *Array
	subarray *C.tiledb_subarray_t
	context  *Context
}

// NewSubarray creates a new subarray for array. It has internal coalesce_ranges == true.
func (a *Array) NewSubarray() (*Subarray, error) {
	var sa *C.tiledb_subarray_t

	ret := C.tiledb_subarray_alloc(a.context.tiledbContext, a.tiledbArray, &sa)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating Subarray: %s", a.context.LastError())
	}

	subarray := &Subarray{array: a, subarray: sa, context: a.context}
	freeOnGC(subarray)

	return subarray, nil
}

// SetConfig sets the subarray config. Currently it overrides only sm.read_range_oob
func (sa *Subarray) SetConfig(cfg *Config) error {
	ret := C.tiledb_subarray_set_config(sa.context.tiledbContext, sa.subarray, cfg.tiledbConfig)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting Config: %s", sa.context.LastError())
	}
	return nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be called earlier to
// manually release memory if needed. Free is idempotent and can safely be called many times on the same object.
func (sa *Subarray) Free() {
	if sa.subarray != nil {
		C.tiledb_subarray_free(&sa.subarray)
	}
}

// SetCoalesceRanges sets coalesce_ranges property on a TileDB subarray object.
// Intended to be used just after array.NewSubarray to replace the initial coalesce_ranges == true with coalesce_ranges = false if needed.
func (sa *Subarray) SetCoalesceRanges(b bool) error {
	var coalesce C.int
	if b {
		coalesce = 1
	}

	ret := C.tiledb_subarray_set_coalesce_ranges(sa.context.tiledbContext, sa.subarray, coalesce)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error setting coalesce ranges on subarray: %s", sa.context.LastError())
	}

	return nil
}

// AddRange adds a range along a subarray dimension. It checks the types of range and dimension and
// if the datatype of the range is not the same as the type of the dimension it returns an error.
func (sa *Subarray) AddRange(dimIdx uint32, r Range) error {
	dt, isVar, err := datatypeOfDimensionFromIndex(sa.array, dimIdx)
	if err != nil {
		return err
	}
	if err := r.assertCompatibility(dt, isVar); err != nil {
		return err
	}

	var ret C.int32_t
	if isVar {
		startSlice := []byte(r.start.(string))
		endSlice := []byte(r.end.(string))
		ret = C.tiledb_subarray_add_range_var(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx),
			unsafe.Pointer(&startSlice[0]), C.uint64_t(len(startSlice)), unsafe.Pointer(&endSlice[0]), C.uint64_t(len(endSlice)))
	} else {
		ret = C.tiledb_subarray_add_range(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx),
			addressableValue(r.start).UnsafePointer(), addressableValue(r.end).UnsafePointer(), nil)
	}
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding subarray range: %s", sa.context.LastError())
	}

	return nil
}

// AddRangeByName adds a range along a subarray dimension. It checks the types of range and dimension and
// if the datatype of the range is not the same as the type of the dimension it returns an error.
func (sa *Subarray) AddRangeByName(dimName string, r Range) error {
	dt, isVar, err := datatypeOfDimensionFromName(sa.array, dimName)
	if err != nil {
		return err
	}
	if err := r.assertCompatibility(dt, isVar); err != nil {
		return err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var ret C.int32_t
	if isVar {
		startSlice := []byte(r.start.(string))
		endSlice := []byte(r.end.(string))
		ret = C.tiledb_subarray_add_range_var_by_name(sa.context.tiledbContext, sa.subarray, cDimName,
			unsafe.Pointer(&startSlice[0]), C.uint64_t(len(startSlice)), unsafe.Pointer(&endSlice[0]), C.uint64_t(len(endSlice)))
	} else {
		ret = C.tiledb_subarray_add_range_by_name(sa.context.tiledbContext, sa.subarray, cDimName,
			addressableValue(r.start).UnsafePointer(), addressableValue(r.end).UnsafePointer(), nil)
	}
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error adding subarray range: %s", sa.context.LastError())
	}

	return nil
}

// GetRangeNum retrieves the number of ranges of the query subarray along a given dimension index.
func (sa *Subarray) GetRangeNum(dimIdx uint32) (uint64, error) {
	var rangeNum uint64

	ret := C.tiledb_subarray_get_range_num(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx), (*C.uint64_t)(unsafe.Pointer(&rangeNum)))
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error retrieving subarray range num: %s", sa.context.LastError())
	}

	return rangeNum, nil
}

// GetRangeNum retrieves the number of ranges of the query subarray along a given dimension name.
func (sa *Subarray) GetRangeNumFromName(dimName string) (uint64, error) {
	var rangeNum uint64

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	ret := C.tiledb_subarray_get_range_num_from_name(sa.context.tiledbContext, sa.subarray, cDimName, (*C.uint64_t)(unsafe.Pointer(&rangeNum)))
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error retrieving subarray range num: %s", sa.context.LastError())
	}

	return rangeNum, nil
}

// GetRange retrieves a specific range of the subarray along a given dimension index.
func (sa *Subarray) GetRange(dimIdx uint32, rangeNum uint64) (Range, error) {
	dt, isVar, err := datatypeOfDimensionFromIndex(sa.array, dimIdx)
	if err != nil {
		return Range{}, err
	}

	var r Range
	var ret C.int32_t
	if isVar {
		var startSize, endSize uint64
		ret = C.tiledb_subarray_get_range_var_size(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx), C.uint64_t(rangeNum),
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
			ret = C.tiledb_subarray_get_range_var(sa.context.tiledbContext, sa.subarray,
				C.uint32_t(dimIdx), C.uint64_t(rangeNum), sp, ep)
			if ret == C.TILEDB_OK {
				r.start = string(startData)
				r.end = string(endData)
			}
		}
	} else {
		var startPointer, endPointer, stridePointer unsafe.Pointer
		ret = C.tiledb_subarray_get_range(sa.context.tiledbContext, sa.subarray,
			C.uint32_t(dimIdx), C.uint64_t(rangeNum), &startPointer, &endPointer, &stridePointer)
		typ := dt.ReflectType()
		if ret == C.TILEDB_OK {
			r.start = reflect.NewAt(typ, startPointer).Elem().Interface()
			r.end = reflect.NewAt(typ, endPointer).Elem().Interface()
		}
	}
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("Error retrieving subarray range for dimension %d and range num %d: %s", dimIdx, rangeNum, sa.context.LastError())
	}

	return r, err
}

// GetRangeFromName retrieves a specific range of the subarray along a given dimension name.
func (sa *Subarray) GetRangeFromName(dimName string, rangeNum uint64) (Range, error) {
	dt, isVar, err := datatypeOfDimensionFromName(sa.array, dimName)
	if err != nil {
		return Range{}, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var r Range
	var ret C.int32_t
	if isVar {
		var startSize, endSize uint64
		ret = C.tiledb_subarray_get_range_var_size_from_name(sa.context.tiledbContext, sa.subarray, cDimName, C.uint64_t(rangeNum),
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
			ret = C.tiledb_subarray_get_range_var_from_name(sa.context.tiledbContext, sa.subarray,
				cDimName, C.uint64_t(rangeNum), sp, ep)
			if ret == C.TILEDB_OK {
				r.start = string(startData)
				r.end = string(endData)
			}
		}
	} else {
		var startPointer, endPointer, stridePointer unsafe.Pointer
		ret = C.tiledb_subarray_get_range_from_name(sa.context.tiledbContext, sa.subarray,
			cDimName, C.uint64_t(rangeNum), &startPointer, &endPointer, &stridePointer)
		typ := dt.ReflectType()
		if ret == C.TILEDB_OK {
			r.start = reflect.NewAt(typ, startPointer).Elem().Interface()
			r.end = reflect.NewAt(typ, endPointer).Elem().Interface()
		}
	}
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("Error retrieving subarray range for dimension %s and range num %d: %s", dimName, rangeNum, sa.context.LastError())
	}

	return r, err
}
