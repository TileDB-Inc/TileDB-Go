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
	"runtime"
	"unsafe"
)

// Filter represents
type Filter struct {
	tiledbFilter *C.tiledb_filter_t
	context      *Context
}

// NewFilter Allocs a new filter
func NewFilter(context *Context, filterType FilterType) (*Filter, error) {
	filter := Filter{context: context}

	ret := C.tiledb_filter_alloc(filter.context.tiledbContext, C.tiledb_filter_type_t(filterType), &filter.tiledbFilter)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb filter: %s", filter.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&filter, func(filter *Filter) {
		filter.Free()
	})

	return &filter, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (f *Filter) Free() {
	if f.tiledbFilter != nil {
		C.tiledb_filter_free(&f.tiledbFilter)
	}
}

// Type returns the filter type
func (f *Filter) Type() (FilterType, error) {
	var filterType C.tiledb_filter_type_t
	ret := C.tiledb_filter_get_type(f.context.tiledbContext, f.tiledbFilter, &filterType)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting tiledb filter type: %s", f.context.LastError())
	}

	return FilterType(filterType), nil
}

// SetOption set an option on a filter. Options are filter dependent;
// this function returns an error if the given option is not valid for the
// given filter.
func (f *Filter) SetOption(filterOption FilterOption, valueInterface interface{}) error {
	var cvalue unsafe.Pointer

	switch filterOption {
	case TILEDB_COMPRESSION_LEVEL:
		value, ok := valueInterface.(int32)
		if !ok {
			return fmt.Errorf("Error setting tiledb filter option TILEDB_COMPRESSION_LEVEL, passed data is not int32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error setting tiledb filter option: %s", f.context.LastError())
		}
	case TILEDB_BIT_WIDTH_MAX_WINDOW:
		value, ok := valueInterface.(uint32)
		if !ok {
			return fmt.Errorf("Error setting tiledb filter option TILEDB_BIT_WIDTH_MAX_WINDOW, passed data is not uint32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error setting tiledb filter option: %s", f.context.LastError())
		}
	case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
		value, ok := valueInterface.(uint32)
		if !ok {
			return fmt.Errorf("Error setting tiledb filter option TILEDB_POSITIVE_DELTA_MAX_WINDOW, passed data is not uint32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("Error setting tiledb filter option: %s", f.context.LastError())
		}
	}

	return nil
}

// Option fetchs the specified option set on a filter. Returns an interface{}
// dependent on the option being fetched
// var optionValue int32
// optionValueInterface, err := filter.Option(TILEDB_FILTER_GZIP)
// optionValue = optionValueInterface.(int32)
func (f *Filter) Option(filterOption FilterOption) (interface{}, error) {

	var cvalue unsafe.Pointer

	switch filterOption {
	case TILEDB_COMPRESSION_LEVEL:
		var val int32
		cvalue = unsafe.Pointer(&val)
		ret := C.tiledb_filter_get_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("Error getting tiledb filter option: %s", f.context.LastError())
		}
		return val, nil
	case TILEDB_BIT_WIDTH_MAX_WINDOW:
		var val uint32
		cvalue = unsafe.Pointer(&val)
		ret := C.tiledb_filter_get_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("Error getting tiledb filter option: %s", f.context.LastError())
		}
		return val, nil
	case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
		var val uint32
		cvalue = unsafe.Pointer(&val)
		ret := C.tiledb_filter_get_option(f.context.tiledbContext, f.tiledbFilter, C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("Error getting tiledb filter option: %s", f.context.LastError())
		}
		return val, nil
	}
	return nil, nil
}
