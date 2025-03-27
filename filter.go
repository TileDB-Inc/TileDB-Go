package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"unsafe"
)

type filterHandle struct{ *capiHandle }

func freeCapiFilter(c unsafe.Pointer) {
	C.tiledb_filter_free((**C.tiledb_filter_t)(unsafe.Pointer(&c)))
}

func newFilterHandle(ptr *C.tiledb_filter_t) filterHandle {
	return filterHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiFilter)}
}

func (x filterHandle) Get() *C.tiledb_filter_t {
	return (*C.tiledb_filter_t)(x.capiHandle.Get())
}

// Filter represents
type Filter struct {
	tiledbFilter filterHandle
	context      *Context
}

func newFilterFromHandle(context *Context, handle filterHandle) *Filter {
	return &Filter{tiledbFilter: handle, context: context}
}

// NewFilter allocates a new filter.
func NewFilter(context *Context, filterType FilterType) (*Filter, error) {
	var filterPtr *C.tiledb_filter_t
	ret := C.tiledb_filter_alloc(context.tiledbContext.Get(), C.tiledb_filter_type_t(filterType), &filterPtr)
	runtime.KeepAlive(context)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb filter: %w", context.LastError())
	}

	return newFilterFromHandle(context, newFilterHandle(filterPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (f *Filter) Free() {
	f.tiledbFilter.Free()
}

// Context exposes the internal TileDB context used to initialize the filter.
func (f *Filter) Context() *Context {
	return f.context
}

// Type returns the filter type.
func (f *Filter) Type() (FilterType, error) {
	var filterType C.tiledb_filter_type_t
	ret := C.tiledb_filter_get_type(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), &filterType)
	runtime.KeepAlive(f)

	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting tiledb filter type: %w", f.context.LastError())
	}

	return FilterType(filterType), nil
}

// SetOption sets an option on a filter. Options are filter dependent;
// this function returns an error if the given option is not valid for the
// given filter.
func (f *Filter) SetOption(filterOption FilterOption, valueInterface interface{}) error {
	var cvalue unsafe.Pointer

	switch filterOption {
	case TILEDB_COMPRESSION_LEVEL:
		value, ok := valueInterface.(int32)
		if !ok {
			return errors.New("error setting tiledb filter option TILEDB_COMPRESSION_LEVEL, passed data is not int32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error setting tiledb filter option: %w", f.context.LastError())
		}
	case TILEDB_BIT_WIDTH_MAX_WINDOW:
		value, ok := valueInterface.(uint32)
		if !ok {
			return errors.New("error setting tiledb filter option TILEDB_BIT_WIDTH_MAX_WINDOW, passed data is not uint32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error setting tiledb filter option: %w", f.context.LastError())
		}
	case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
		value, ok := valueInterface.(uint32)
		if !ok {
			return errors.New("error setting tiledb filter option TILEDB_POSITIVE_DELTA_MAX_WINDOW, passed data is not uint32")
		}
		cvalue = unsafe.Pointer(&value)
		ret := C.tiledb_filter_set_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error setting tiledb filter option: %w", f.context.LastError())
		}
	}
	runtime.KeepAlive(f)

	return nil
}

// Option fetches the specified option set on a filter. Returns an interface{}
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
		ret := C.tiledb_filter_get_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("error getting tiledb filter option: %w", f.context.LastError())
		}
		return val, nil
	case TILEDB_BIT_WIDTH_MAX_WINDOW:
		var val uint32
		cvalue = unsafe.Pointer(&val)
		ret := C.tiledb_filter_get_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("error getting tiledb filter option: %w", f.context.LastError())
		}
		return val, nil
	case TILEDB_POSITIVE_DELTA_MAX_WINDOW:
		var val uint32
		cvalue = unsafe.Pointer(&val)
		ret := C.tiledb_filter_get_option(f.context.tiledbContext.Get(), f.tiledbFilter.Get(), C.tiledb_filter_option_t(filterOption), cvalue)
		if ret != C.TILEDB_OK {
			return nil, fmt.Errorf("error getting tiledb filter option: %w", f.context.LastError())
		}
		return val, nil
	}
	runtime.KeepAlive(f)
	return nil, nil
}
