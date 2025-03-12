package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"reflect"
	"runtime"
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

	ret := C.tiledb_subarray_alloc(a.context.tiledbContext, a.tiledbArray.Get(), &sa)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating Subarray: %w", a.context.LastError())
	}

	subarray := &Subarray{array: a, subarray: sa, context: a.context}
	freeOnGC(subarray)

	return subarray, nil
}

// SetConfig sets the subarray config. Currently it overrides only sm.read_range_oob.
func (sa *Subarray) SetConfig(cfg *Config) error {
	ret := C.tiledb_subarray_set_config(sa.context.tiledbContext, sa.subarray, cfg.tiledbConfig.Get())
	runtime.KeepAlive(sa)
	runtime.KeepAlive(cfg)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting Config: %w", sa.context.LastError())
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

// SetSubArray sets a subarray, defined in the order dimensions were added.
// Coordinates are inclusive. For the case of writes, this is meaningful only
// for dense arrays, and specifically dense writes.
func (sa *Subarray) SetSubArray(subArray interface{}) error {

	if reflect.TypeOf(subArray).Kind() != reflect.Slice {
		return fmt.Errorf("subarray passed must be a slice, type passed was: %s", reflect.TypeOf(subArray).Kind().String())
	}

	subArrayType := reflect.TypeOf(subArray).Elem().Kind()

	schema, err := sa.array.Schema()
	if err != nil {
		return fmt.Errorf("could not get array schema from array: %w", err)
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return fmt.Errorf("could not get domain from array schema: %w", err)
	}
	defer domain.Free()

	domainType, err := domain.Type()
	if err != nil {
		return fmt.Errorf("could not get domain type: %w", err)
	}

	if subArrayType != domainType.ReflectKind() {
		return fmt.Errorf("domain and subarray do not have the same data types. Domain: %s, Extent: %s", domainType.ReflectKind().String(), subArrayType.String())
	}

	var csubArray unsafe.Pointer
	switch subArrayType {
	case reflect.Int:
		// Create subArray void*
		tmpSubArray := subArray.([]int)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Int8:
		// Create subArray void*
		tmpSubArray := subArray.([]int8)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Int16:
		// Create subArray void*
		tmpSubArray := subArray.([]int16)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Int32:
		// Create subArray void*
		tmpSubArray := subArray.([]int32)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Int64:
		// Create subArray void*
		tmpSubArray := subArray.([]int64)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Uint:
		// Create subArray void*
		tmpSubArray := subArray.([]uint)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Uint8:
		// Create subArray void*
		tmpSubArray := subArray.([]uint8)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Uint16:
		// Create subArray void*
		tmpSubArray := subArray.([]uint16)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Uint32:
		// Create subArray void*
		tmpSubArray := subArray.([]uint32)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Uint64:
		// Create subArray void*
		tmpSubArray := subArray.([]uint64)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Float32:
		// Create subArray void*
		tmpSubArray := subArray.([]float32)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Float64:
		// Create subArray void*
		tmpSubArray := subArray.([]float64)
		csubArray = slicePtr(tmpSubArray)
	case reflect.Bool:
		// Create subArray void*
		tmpSubArray := subArray.([]bool)
		csubArray = slicePtr(tmpSubArray)
	default:
		return fmt.Errorf("unrecognized subArray type passed: %s", subArrayType.String())
	}

	ret := C.tiledb_subarray_set_subarray(sa.context.tiledbContext, sa.subarray, csubArray)
	runtime.KeepAlive(sa)
	// csubarray is being kept alive by passing it to cgo call.
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting subarray: %w", sa.context.LastError())
	}
	return nil
}

// SetCoalesceRanges sets coalesce_ranges property on a TileDB subarray object.
// Intended to be used just after array.NewSubarray to replace the initial coalesce_ranges == true with coalesce_ranges = false if needed.
func (sa *Subarray) SetCoalesceRanges(b bool) error {
	var coalesce C.int
	if b {
		coalesce = 1
	}

	ret := C.tiledb_subarray_set_coalesce_ranges(sa.context.tiledbContext, sa.subarray, coalesce)
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting coalesce ranges on subarray: %w", sa.context.LastError())
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
			slicePtr(startSlice), C.uint64_t(len(startSlice)), slicePtr(endSlice), C.uint64_t(len(endSlice)))
	} else {
		startValue := addressableValue(r.start)
		endValue := addressableValue(r.end)
		ret = C.tiledb_subarray_add_range(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx),
			startValue.UnsafePointer(), endValue.UnsafePointer(), nil)
	}
	runtime.KeepAlive(sa)
	// The start and end pointers are being kept alive by passing them to cgo calls.
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding subarray range: %w", sa.context.LastError())
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
			slicePtr(startSlice), C.uint64_t(len(startSlice)), slicePtr(endSlice), C.uint64_t(len(endSlice)))
	} else {
		startValue := addressableValue(r.start)
		endValue := addressableValue(r.end)
		ret = C.tiledb_subarray_add_range_by_name(sa.context.tiledbContext, sa.subarray, cDimName,
			startValue.UnsafePointer(), endValue.UnsafePointer(), nil)
	}
	runtime.KeepAlive(sa)
	// The start and end pointers are being kept alive by passing them to cgo calls.
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding subarray range: %w", sa.context.LastError())
	}

	return nil
}

// GetRangeNum retrieves the number of ranges of the query subarray along a given dimension index.
func (sa *Subarray) GetRangeNum(dimIdx uint32) (uint64, error) {
	var rangeNum uint64

	ret := C.tiledb_subarray_get_range_num(sa.context.tiledbContext, sa.subarray, C.uint32_t(dimIdx), (*C.uint64_t)(unsafe.Pointer(&rangeNum)))
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error retrieving subarray range num: %w", sa.context.LastError())
	}

	return rangeNum, nil
}

// GetRangeNum retrieves the number of ranges of the query subarray along a given dimension name.
func (sa *Subarray) GetRangeNumFromName(dimName string) (uint64, error) {
	var rangeNum uint64

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	ret := C.tiledb_subarray_get_range_num_from_name(sa.context.tiledbContext, sa.subarray, cDimName, (*C.uint64_t)(unsafe.Pointer(&rangeNum)))
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error retrieving subarray range num: %w", sa.context.LastError())
	}

	return rangeNum, nil
}

// GetRanges gets the number of dimensions from the array under current subarray
// and builds an array of dimensions that have as memmbers arrays of ranges.
func (s *Subarray) GetRanges() (map[string][]Range, error) {
	// We need to infer the datatype of the dimension represented by index
	// dimIdx. That said:
	// Get array schema
	schema, err := s.array.Schema()
	if err != nil {
		return nil, err
	}
	defer schema.Free()

	// Get the domain object
	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}
	defer domain.Free()

	// Use the index to retrieve the dimension object
	nDim, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	var dimIdx uint

	rangeMap := make(map[string][]Range)
	for dimIdx = 0; dimIdx < nDim; dimIdx++ {
		err = func() error {
			// Get dimension object
			dimension, err := domain.DimensionFromIndex(dimIdx)
			if err != nil {
				return err
			}
			defer dimension.Free()

			// Get name from dimension
			name, err := dimension.Name()
			if err != nil {
				return err
			}

			// Get number of renges to iterate
			numOfRanges, err := s.GetRangeNum(uint32(dimIdx))
			if err != nil {
				return err
			}

			var I uint64
			rangeArray := make([]Range, 0)
			for I = 0; I < numOfRanges; I++ {

				r, err := s.GetRange(uint32(dimIdx), I)
				if err != nil {
					return err
				}
				// Append range to range Array
				rangeArray = append(rangeArray, r)
			}
			// key: name (string), value: rangeArray ([]RangeLimits)
			rangeMap[name] = rangeArray

			return nil
		}()

		if err != nil {
			return nil, err
		}
	}

	return rangeMap, err
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
		if startSize == 0 && endSize == 0 {
			r.start = ""
			r.end = ""
		} else if ret == C.TILEDB_OK {
			startData := make([]byte, int(startSize))
			sp := slicePtr(startData)
			endData := make([]byte, int(endSize))
			ep := slicePtr(endData)

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
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("error retrieving subarray range for dimension %d and range num %d: %w", dimIdx, rangeNum, sa.context.LastError())
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
		if startSize == 0 && endSize == 0 {
			r.start = ""
			r.end = ""
		} else if ret == C.TILEDB_OK {
			startData := make([]byte, int(startSize))
			sp := slicePtr(startData)
			endData := make([]byte, int(endSize))
			ep := slicePtr(endData)

			ret = C.tiledb_subarray_get_range_var_from_name(sa.context.tiledbContext, sa.subarray,
				cDimName, C.uint64_t(rangeNum), sp, ep)
			// startData and endData are being kept alive by passing them to the cgo call.
			if ret == C.TILEDB_OK {
				r.start = string(startData)
				r.end = string(endData)
			}
		}
	} else {
		var startPointer, endPointer, stridePointer unsafe.Pointer // sa must be kept alive while these pointers are being accessed.
		ret = C.tiledb_subarray_get_range_from_name(sa.context.tiledbContext, sa.subarray,
			cDimName, C.uint64_t(rangeNum), &startPointer, &endPointer, &stridePointer)
		typ := dt.ReflectType()
		if ret == C.TILEDB_OK {
			r.start = reflect.NewAt(typ, startPointer).Elem().Interface()
			r.end = reflect.NewAt(typ, endPointer).Elem().Interface()
		}
	}
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return Range{}, fmt.Errorf("error retrieving subarray range for dimension %s and range num %d: %w", dimName, rangeNum, sa.context.LastError())
	}

	return r, err
}
