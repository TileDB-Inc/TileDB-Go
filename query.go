package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"unsafe"

	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

type queryState struct {
	ptr    *C.tiledb_query_t
	pinner runtime.Pinner
}

func freeCapiQueryState(c unsafe.Pointer) {
	h := (*queryState)(c)
	C.tiledb_query_free(&h.ptr)
	h.pinner.Unpin()
}

type queryHandle struct{ *capiHandle }

func newQueryHandle(ptr *C.tiledb_query_t) queryHandle {
	state := &queryState{ptr: ptr}
	return queryHandle{newCapiHandle(unsafe.Pointer(state), freeCapiQueryState)}
}

func (x queryHandle) getState() *queryState {
	return (*queryState)(x.capiHandle.Get())
}

func (x queryHandle) Get() *C.tiledb_query_t {
	return x.getState().ptr
}

func (x queryHandle) Pin(pointer any) {
	x.getState().pinner.Pin(pointer)
}

// Query construct and execute read/write queries on a tiledb Array
type Query struct {
	tiledbQuery          queryHandle
	array                *Array
	context              *Context
	config               *Config
	bufferMutex          sync.Mutex
	resultBufferElements map[string][3]*uint64
}

func newQueryFromHandle(context *Context, array *Array, handle queryHandle) *Query {
	return &Query{tiledbQuery: handle, array: array, context: context, resultBufferElements: make(map[string][3]*uint64)}
}

// RangeLimits defines a query range
type RangeLimits struct {
	start interface{}
	end   interface{}
}

// MarshalJSON implements the Marshaler interface for RangeLimits.
func (r RangeLimits) MarshalJSON() ([]byte, error) {
	rangeLimitMap := make(map[string]interface{})
	rangeLimitMap["end"] = r.end
	rangeLimitMap["start"] = r.start

	return json.Marshal(rangeLimitMap)
}

/*
NewQuery creates a TileDB query object.

If the provided Context is nil, the context of the Array is used instead.
The storage manager also acquires a shared lock on the array.
This means multiple read and write queries to the same array can be made
concurrently (in TileDB, only consolidation requires an exclusive lock for
a short period of time).
*/
func NewQuery(tdbCtx *Context, array *Array) (*Query, error) {
	if array == nil {
		return nil, errors.New("error creating tiledb query: passed array is nil")
	}
	if tdbCtx == nil {
		tdbCtx = array.context
	}

	queryType, err := array.QueryType()
	if err != nil {
		return nil, fmt.Errorf("error getting QueryType from passed array %w", err)
	}

	var queryPtr *C.tiledb_query_t
	ret := C.tiledb_query_alloc(tdbCtx.tiledbContext.Get(), array.tiledbArray.Get(), C.tiledb_query_type_t(queryType), &queryPtr)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb query: %w", tdbCtx.LastError())
	}

	return newQueryFromHandle(tdbCtx, array, newQueryHandle(queryPtr)), nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (q *Query) Free() {
	q.tiledbQuery.Free()
}

// Context exposes the internal TileDB context used to initialize the query.
func (q *Query) Context() *Context {
	return q.context
}

// GetRanges gets the number of dimensions from the array under current query
// and builds an array of dimensions that have as memmbers arrays of ranges.
//
// Deprecated: Use Subarrays
func (q *Query) GetRanges() (map[string][]RangeLimits, error) {
	// We need to infer the datatype of the dimension represented by index
	// dimIdx. That said:
	// Get array schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, err
	}

	// Get the domain object
	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}

	// Use the index to retrieve the dimension object
	nDim, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	subarray, err := q.GetSubarray()
	if err != nil {
		return nil, err
	}

	var dimIdx uint

	rangeMap := make(map[string][]RangeLimits)
	for dimIdx = 0; dimIdx < nDim; dimIdx++ {
		// Get dimension object
		dimension, err := domain.DimensionFromIndex(dimIdx)
		if err != nil {
			return nil, err
		}

		// Get name from dimension
		name, err := dimension.Name()
		if err != nil {
			return nil, err
		}

		// Get number of renges to iterate
		numOfRanges, err := subarray.GetRangeNum(uint32(dimIdx))
		if err != nil {
			return nil, err
		}

		var I uint64
		rangeArray := make([]RangeLimits, 0)
		for I = 0; I < numOfRanges; I++ {

			r, err := subarray.GetRange(uint32(dimIdx), I)
			if err != nil {
				return nil, err
			}
			// Append range to range Array
			rangeArray = append(rangeArray, RangeLimits(r))
		}
		// key: name (string), value: rangeArray ([]RangeLimits)
		rangeMap[name] = rangeArray
	}

	return rangeMap, err
}

// ResultBufferElements returns the number of elements in the result buffers
// from a read query.
// This is a map from the attribute name to a pair of values.
// The first is number of elements (offsets) for var size attributes, and the
// second is number of elements in the data buffer. For fixed sized attributes
// (and coordinates), the first is always 0.
func (q *Query) ResultBufferElements() (map[string][3]uint64, error) {
	elements := make(map[string][3]uint64)

	// Will need the schema to infer data type size for attributes
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf("could not get schema for ResultBufferElements: %w", err)
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("could not get domain for ResultBufferElements: %w", err)
	}
	defer domain.Free()

	var datatype Datatype
	for attributeOrDimension, v := range q.resultBufferElements {
		// Handle coordinates
		if attributeOrDimension == TILEDB_COORDS {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)

			domainType, err := domain.Type()
			if err != nil {
				return nil, fmt.Errorf("could not get domainType for ResultBufferElements: %w", err)
			}

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / domainType.Size()
			elements[attributeOrDimension] = [3]uint64{offsetElements, bufferElements, 0}
		} else {
			// For fixed length attributes offset elements are always zero
			offsetElements := uint64(0)
			if v[0] != nil {
				// The attribute is variable length
				offsetElements = (*v[0]) / bytesizes.Uint64
			}

			validityElements := uint64(0)
			if v[2] != nil {
				// The attribute is nullable
				validityElements = (*v[2]) / bytesizes.Uint8
			}

			hasDim, err := domain.HasDimension(attributeOrDimension)
			if err != nil {
				return nil, err
			}

			hasAttr, err := schema.HasAttribute(attributeOrDimension)
			if err != nil {
				return nil, err
			}

			hasDimLabel, err := schema.HasDimensionLabel(attributeOrDimension)
			if err != nil {
				return nil, err
			}

			if hasDim {
				dimension, err := domain.DimensionFromName(attributeOrDimension)
				if err != nil {
					return nil, fmt.Errorf("could not get attribute or dimension for SetBuffer: %s", attributeOrDimension)
				}

				datatype, err = dimension.Type()
				if err != nil {
					return nil, fmt.Errorf("could not get dimensionType for SetBuffer: %s", attributeOrDimension)
				}

				dimension.Free()
			} else if hasAttr {
				// Get the attribute
				attribute, err := schema.AttributeFromName(attributeOrDimension)
				if err != nil {
					return nil, fmt.Errorf("could not get attribute %s for ResultBufferElements: %w", attributeOrDimension, err)
				}

				// Get datatype size to convert byte lengths to needed buffer sizes
				datatype, err = attribute.Type()
				if err != nil {
					return nil, fmt.Errorf("could not get attribute type for ResultBufferElements: %w", err)
				}

				attribute.Free()
			} else if hasDimLabel {
				datatype, err = q.getDimensionLabelDataType(attributeOrDimension)
				if err != nil {
					return nil, fmt.Errorf("could not get dimension label type for ResultBufferElements: %w", err)
				}
			} else {
				return nil, fmt.Errorf("error in ResultBufferElements for %s: "+
					"Attribute/dimension/label does not exist", attributeOrDimension)
			}

			// Number of buffer elements is calculated
			bufferElements := (*v[1]) / datatype.Size()
			elements[attributeOrDimension] = [3]uint64{offsetElements, bufferElements, validityElements}
		}
	}

	return elements, nil
}

// SetLayout sets the layout of the cells to be written or read.
func (q *Query) SetLayout(layout Layout) error {
	ret := C.tiledb_query_set_layout(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), C.tiledb_layout_t(layout))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting query layout: %w", q.context.LastError())
	}
	runtime.KeepAlive(q)
	return nil
}

// SetQueryCondition sets a query condition on a read query.
func (q *Query) SetQueryCondition(cond *QueryCondition) error {
	if ret := C.tiledb_query_set_condition(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), cond.cond.Get()); ret != C.TILEDB_OK {
		return fmt.Errorf("error getting config from query: %w", q.context.LastError())
	}
	runtime.KeepAlive(q)
	return nil
}

// Finalize flushes all internal state of a query object and finalizes the
// query. This is applicable only to global layout writes. It has no effect
// for any other query type.
func (q *Query) Finalize() error {
	ret := C.tiledb_query_finalize(q.context.tiledbContext.Get(), q.tiledbQuery.Get())
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error finalizing query: %w", q.context.LastError())
	}
	return nil
}

/*
Submit a TileDB query
This will block until query is completed

Note:
Finalize() must be invoked after finish writing in global layout
(via repeated invocations of Submit()), in order to flush any internal state.
For the case of reads, if the returned status is TILEDB_INCOMPLETE, TileDB
could not fit the entire result in the user’s buffers. In this case, the user
should consume the read results (if any), optionally reset the buffers with
SetBuffer(), and then resubmit the query until the status becomes
TILEDB_COMPLETED. If all buffer sizes after the termination of this
function become 0, then this means that no useful data was read into
the buffers, implying that the larger buffers are needed for the query
to proceed. In this case, the users must reallocate their buffers
(increasing their size), reset the buffers with set_buffer(),
and resubmit the query.
*/
func (q *Query) Submit() error {
	ret := C.tiledb_query_submit(q.context.tiledbContext.Get(), q.tiledbQuery.Get())
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error submitting query: %w", q.context.LastError())
	}

	return nil
}

// Status returns the status of a query.
func (q *Query) Status() (QueryStatus, error) {
	var status C.tiledb_query_status_t
	ret := C.tiledb_query_get_status(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &status)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting query status: %w", q.context.LastError())
	}
	return QueryStatus(status), nil
}

// Type returns the query type.
func (q *Query) Type() (QueryType, error) {
	var queryType C.tiledb_query_type_t
	ret := C.tiledb_query_get_type(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &queryType)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting query type: %w", q.context.LastError())
	}
	return QueryType(queryType), nil
}

// HasResults returns true if the query has results.
// Applicable only to read queries (it returns false for write queries).
func (q *Query) HasResults() (bool, error) {
	var hasResults C.int32_t
	ret := C.tiledb_query_has_results(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &hasResults)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error checking if query has results: %w", q.context.LastError())
	}
	return int(hasResults) == 1, nil
}

// EstResultSize gets the query estimated result size in bytes for an attribute.
func (q *Query) EstResultSize(attributeName string) (*uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var size uint64

	ret := C.tiledb_query_get_est_result_size(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&size)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error estimating query result size: %w", q.context.LastError())
	}

	return &size, nil
}

// EstResultSizeVar gets the query estimated result size in bytes for a var sized attribute.
func (q *Query) EstResultSizeVar(attributeName string) (*uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var sizeOff, sizeVal uint64

	ret := C.tiledb_query_get_est_result_size_var(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&sizeOff)),
		(*C.uint64_t)(unsafe.Pointer(&sizeVal)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error estimating query result var size: %w", q.context.LastError())
	}

	return &sizeOff, &sizeVal, nil
}

// EstResultSizeNullable gets the query estimated result size in bytes for an attribute.
func (q *Query) EstResultSizeNullable(attributeName string) (*uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var size, sizeValidity uint64

	ret := C.tiledb_query_get_est_result_size_nullable(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&size)),
		(*C.uint64_t)(unsafe.Pointer(&sizeValidity)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error estimating query result size: %w", q.context.LastError())
	}

	return &size, &sizeValidity, nil
}

// EstResultSizeVarNullable gets the query estimated result size in bytes for a var sized attribute.
func (q *Query) EstResultSizeVarNullable(attributeName string) (*uint64, *uint64, *uint64, error) {
	cAttributeName := C.CString(attributeName)
	defer C.free(unsafe.Pointer(cAttributeName))

	var sizeOff, sizeVal, sizeValidity uint64

	ret := C.tiledb_query_get_est_result_size_var_nullable(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeName,
		(*C.uint64_t)(unsafe.Pointer(&sizeOff)),
		(*C.uint64_t)(unsafe.Pointer(&sizeVal)),
		(*C.uint64_t)(unsafe.Pointer(&sizeValidity)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, nil, nil, fmt.Errorf("error estimating query result var size: %w", q.context.LastError())
	}

	return &sizeOff, &sizeVal, &sizeValidity, nil
}

/*
EstimateBufferElements computes an upper bound on the buffer elements needed to
read a subarray or range(s).
Returns a map of attribute or dimension name to the maximum
number of elements that can be read in the given subarray. For each attribute,
a pair of numbers are returned. The first, for variable-length attributes, is
the maximum number of offsets for that attribute in the given subarray. For
fixed-length attributes and coordinates, the first is always 0. The second
is the maximum number of elements for that attribute in the given subarray.
*/
func (q *Query) EstimateBufferElements() (map[string][3]uint64, error) {
	// Build map
	ret := make(map[string][3]uint64)
	// Get schema
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
	}
	defer schema.Free()

	attributes, err := schema.Attributes()
	if err != nil {
		return nil, fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
	}
	// Loop through each attribute
	for _, attribute := range attributes {
		// Wrap the body of the for loop in a function to be sure resources are freed by defer calls.
		err := func() error {
			defer attribute.Free()

			// Check if attribute is variable attribute or not
			cellValNum, err := attribute.CellValNum()
			if err != nil {
				return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
			}

			// Get datatype size to convert byte lengths to needed buffer sizes
			dataType, err := attribute.Type()
			if err != nil {
				return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
			}

			dataTypeSize := dataType.Size()

			// Get attribute name
			name, err := attribute.Name()
			if err != nil {
				return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
			}

			nullable, err := attribute.Nullable()
			if err != nil {
				return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
			}

			if cellValNum == TILEDB_VAR_NUM {
				if nullable {
					bufferOffsetSize, bufferValSize, bufferValiditySize, err := q.EstResultSizeVarNullable(name)
					if err != nil {
						return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
					}
					// Set sizes for attribute in return map
					ret[name] = [3]uint64{
						*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
						*bufferValSize / dataTypeSize,
						*bufferValiditySize / bytesizes.Uint8}
				} else {
					bufferOffsetSize, bufferValSize, err := q.EstResultSizeVar(name)
					if err != nil {
						return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
					}
					// Set sizes for attribute in return map
					ret[name] = [3]uint64{
						*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
						*bufferValSize / dataTypeSize,
						0}
				}
			} else {
				if nullable {
					bufferValSize, bufferValiditySize, err := q.EstResultSizeNullable(name)
					if err != nil {
						return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
					}
					ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize,
						*bufferValiditySize / bytesizes.Uint8}
				} else {
					bufferValSize, err := q.EstResultSize(name)
					if err != nil {
						return fmt.Errorf("error getting EstimateBufferElements for array: %w", err)
					}
					ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize, 0}
				}
			}

			return nil
		}()

		if err != nil {
			return nil, err
		}
	}

	// Handle coordinates
	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("could not get domain for EstimateBufferElements: %w", err)
	}
	defer domain.Free()

	ndims, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	for dimIdx := uint(0); dimIdx < ndims; dimIdx++ {
		err = func() error {
			dim, err := domain.DimensionFromIndex(dimIdx)
			if err != nil {
				return err
			}
			defer dim.Free()

			dimType, err := dim.Type()
			if err != nil {
				return err
			}

			dataTypeSize := dimType.Size()

			cellValNum, err := dim.CellValNum()
			if err != nil {
				return fmt.Errorf("error getting MaxBufferElements for array: %w", err)
			}

			// Get dimension name
			name, err := dim.Name()
			if err != nil {
				return fmt.Errorf("error getting MaxBufferElements for array: %w", err)
			}

			if cellValNum == TILEDB_VAR_NUM {
				bufferOffsetSize, bufferValSize, err := q.EstResultSizeVar(name)
				if err != nil {
					return fmt.Errorf("error getting MaxBufferElements for array: %w", err)
				}
				// Set sizes for dimension in return map
				ret[name] = [3]uint64{
					*bufferOffsetSize / uint64(C.TILEDB_OFFSET_SIZE),
					*bufferValSize / dataTypeSize, 0}
			} else {
				bufferValSize, err := q.EstResultSize(name)
				if err != nil {
					return fmt.Errorf("error getting MaxBufferElements for array: %w", err)
				}
				ret[name] = [3]uint64{0, *bufferValSize / dataTypeSize, 0}
			}

			return nil
		}()

		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// GetFragmentNum returns num of fragments.
func (q *Query) GetFragmentNum() (*uint32, error) {
	var num uint32

	ret := C.tiledb_query_get_fragment_num(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		(*C.uint32_t)(unsafe.Pointer(&num)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting num of fragments: %w", q.context.LastError())
	}

	return &num, nil
}

// GetFragmentURI returns the uri for a fragment.
func (q *Query) GetFragmentURI(num uint64) (*string, error) {
	var cURI *C.char // q must be kept alive while cURI is being accessed.

	ret := C.tiledb_query_get_fragment_uri(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		(C.uint64_t)(num),
		&cURI)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error uri for fragment : %d", q.context.LastError())
	}

	uri := C.GoString(cURI)
	runtime.KeepAlive(q)

	return &uri, nil

}

// GetFragmentTimestampRange returns timestamp range for a fragment.
func (q *Query) GetFragmentTimestampRange(num uint64) (*uint64, *uint64, error) {
	var t1, t2 uint64

	ret := C.tiledb_query_get_fragment_timestamp_range(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		(C.uint64_t)(num),
		(*C.uint64_t)(unsafe.Pointer(&t1)),
		(*C.uint64_t)(unsafe.Pointer(&t2)))
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, nil, fmt.Errorf("error getting fragment timestamp: %w", q.context.LastError())
	}

	return &t1, &t2, nil
}

// Array returns array used by query.
func (q *Query) Array() (*Array, error) {
	var arrayPtr *C.tiledb_array_t
	ret := C.tiledb_query_get_array(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &arrayPtr)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting array from query: %w", q.context.LastError())
	}
	return newArrayFromHandle(q.context, newArrayHandle(arrayPtr)), nil
}

// SetConfig sets the config of query.
func (q *Query) SetConfig(config *Config) error {
	q.config = config

	ret := C.tiledb_query_set_config(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), q.config.tiledbConfig.Get())
	runtime.KeepAlive(q)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting config on query: %w", q.context.LastError())
	}

	return nil
}

// Config gets the config of query.
func (q *Query) Config() (*Config, error) {
	var configPtr *C.tiledb_config_t
	ret := C.tiledb_query_get_config(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &configPtr)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting config from query: %w", q.context.LastError())
	}

	return newConfigFromHandle(newConfigHandle(configPtr)), nil
}

// Stats gets stats for a query as json bytes.
func (q *Query) Stats() ([]byte, error) {
	var stats *C.char
	if ret := C.tiledb_query_get_stats(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &stats); ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting stats from query: %w", q.context.LastError())
	}
	runtime.KeepAlive(q)

	s := C.GoString(stats)
	if ret := C.tiledb_stats_free_str(&stats); ret != C.TILEDB_OK {
		return nil, errors.New("error freeing string from dumping stats to string")
	}

	if s == "" {
		return []byte("{}"), nil
	}

	return []byte(s), nil
}

// setResultBufferPointer sets the resultBufferElements for attribute
// pos = 0(offsets buffer) 1(data buffer) 2(validities buffer)
// The caller must hold the q.bufferMutex lock
func (q *Query) setResultBufferPointer(attribute string, pos int, ptr *uint64) {
	ptrs, present := q.resultBufferElements[attribute]
	if !present {
		ptrs = [3]*uint64{nil, nil, nil}
	}
	ptrs[pos] = ptr
	q.resultBufferElements[attribute] = ptrs
}

// SetDataBufferUnsafe sets the buffer for a fixed-sized attribute to a query.
// This takes an unsafe pointer which is passsed straight to tiledb c_api for advanced usage.
func (q *Query) SetDataBufferUnsafe(attribute string, buffer unsafe.Pointer, bufferSize uint64) (*uint64, error) {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	q.tiledbQuery.Pin(buffer)
	q.tiledbQuery.Pin(&bufferSize)

	ret := C.tiledb_query_set_data_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttribute,
		buffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))
	runtime.KeepAlive(q)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query data buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attribute, 1, &bufferSize)

	return &bufferSize, nil
}

// SetDataBuffer sets the buffer for a fixed-sized attribute to a query.
func (q *Query) SetDataBuffer(attributeOrDimension string, buffer interface{}) (*uint64, error) {
	bufferReflectType := reflect.TypeOf(buffer)
	bufferReflectValue := reflect.ValueOf(buffer)
	if bufferReflectValue.Kind() != reflect.Slice {
		return nil, fmt.Errorf("buffer passed must be a slice that is pre-allocated, type passed was: %s",
			bufferReflectValue.Kind().String())
	}

	// Next get the attribute to validate the buffer type is the same as the attribute
	schema, err := q.array.Schema()
	if err != nil {
		return nil, fmt.Errorf("could not get array schema for SetDataBuffer: %w", err)
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return nil, fmt.Errorf("could not get domain for SetDataBuffer: %s", attributeOrDimension)
	}
	defer domain.Free()

	var attributeOrDimensionType Datatype
	// If we are setting tiledb coordinates for a sparse array we want to check
	// the domain type. The TILEDB_COORDS attribute is only materialized after
	// the first write
	if attributeOrDimension == TILEDB_COORDS {
		attributeOrDimensionType, err = domain.Type()
		if err != nil {
			return nil, fmt.Errorf("could not get domainType for SetDataBuffer: %s", attributeOrDimension)
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, err
		}

		hasAttribute, err := schema.HasAttribute(attributeOrDimension)
		if err != nil {
			return nil, err
		}

		hasDimLabel, err := schema.HasDimensionLabel(attributeOrDimension)
		if err != nil {
			return nil, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("could not get attribute or dimension for SetDataBuffer: %s",
					attributeOrDimension)
			}
			defer dimension.Free()

			attributeOrDimensionType, err = dimension.Type()
			if err != nil {
				return nil, fmt.Errorf("could not get dimensionType for SetDataBuffer: %s",
					attributeOrDimension)
			}
		} else if hasAttribute {
			schemaAttribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("could not get attribute %s for SetDataBuffer",
					attributeOrDimension)
			}
			defer schemaAttribute.Free()

			attributeOrDimensionType, err = schemaAttribute.Type()
			if err != nil {
				return nil, fmt.Errorf("could not get attributeType for SetDataBuffer: %s",
					attributeOrDimension)
			}
		} else if hasDimLabel {
			attributeOrDimensionType, err = q.getDimensionLabelDataType(attributeOrDimension)
			if err != nil {
				return nil, fmt.Errorf("could not get dimension label type for SetDataBuffer: %s",
					attributeOrDimension)
			}
		} else {
			return nil, fmt.Errorf("error in SetDataBuffer for %s: "+
				"Attribute/dimension/label does not exist", attributeOrDimension)
		}
	}

	bufferType := bufferReflectType.Elem().Kind()
	if attributeOrDimensionType.ReflectKind() != bufferType {
		return nil, fmt.Errorf("buffer and attribute do not have the same data types. Buffer: %s, Attribute: %s",
			bufferType.String(),
			attributeOrDimensionType.ReflectKind().String())
	}

	cbuffer := bufferReflectValue.UnsafePointer()
	q.tiledbQuery.Pin(cbuffer)
	// Get length of slice, this will be multiplied by size of datatype below
	bufferSize := uint64(bufferReflectValue.Len())
	q.tiledbQuery.Pin(&bufferSize)

	if bufferSize == uint64(0) {
		return nil, errors.New("Buffer has no length, vbuffers are required to be initialized before reading or writting")
	}

	// Acquire a lock to make appending to buffer slice thread safe
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	switch bufferType {
	case reflect.Int:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int
	case reflect.Int8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int8
	case reflect.Int16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int16
	case reflect.Int32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int32
	case reflect.Int64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Int64
	case reflect.Uint:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint
	case reflect.Uint8:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint8
	case reflect.Uint16:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint16
	case reflect.Uint32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint32
	case reflect.Uint64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Uint64
	case reflect.Float32:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float32
	case reflect.Float64:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Float64
	case reflect.Bool:
		// Set buffersize
		bufferSize = bufferSize * bytesizes.Bool
	default:
		return nil,
			fmt.Errorf("unrecognized buffer type passed: %s",
				bufferType.String())
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	ret := C.tiledb_query_set_data_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeOrDimension,
		cbuffer,
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))
	runtime.KeepAlive(q)
	// cbuffer is being kept alive by passing it to cgo call.

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query data buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attributeOrDimension, 1, &bufferSize)

	return &bufferSize, nil

}

// GetDataBuffer retrieves the data buffer of an attribute/dimension.
func (q *Query) GetDataBuffer(attributeOrDimension string) (interface{}, error) {
	buf, _, err := q.getDataBufferAndSize(attributeOrDimension)
	return buf, err
}

// GetExpectedDataBufferLength retrieves the size of the data buffer of an attribute/dimension.
// This is equivalent to calling GetDataBuffer and taking the length of the returned buffer except
// in the case of a deserialized server side read query where GetDataBuffer returns nil.
// Serialization of server side read queries serializes
// only lengths not buffers. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) GetExpectedDataBufferLength(attributeOrDimension string) (uint64, error) {
	_, n, err := q.getDataBufferAndSize(attributeOrDimension)
	return n, err
}

// getDataBufferAndSize uses tiledb.get_query_data_buffer to retrieve the data buffer and its size for an attribute/dimension
// The returned length is equal to the size of the buffer except in the case of a deserialized read query.
// Serialization of read queries serializes only lengths not buffers thus the methods returns a nil buffer and
// a non zero length. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) getDataBufferAndSize(attributeOrDimension string) (interface{}, uint64, error) {
	var datatype Datatype
	schema, err := q.array.Schema()
	if err != nil {
		return nil, 0, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, 0, fmt.Errorf("could not get domain from array schema for GetDataBuffer: %w", err)
	}

	if attributeOrDimension == TILEDB_COORDS {
		datatype, err = domain.Type()
		if err != nil {
			return nil, 0, err
		}
	} else {
		hasDim, err := domain.HasDimension(attributeOrDimension)
		if err != nil {
			return nil, 0, err
		}

		hasAttr, err := schema.HasAttribute(attributeOrDimension)
		if err != nil {
			return nil, 0, err
		}

		hasDimLabel, err := schema.HasDimensionLabel(attributeOrDimension)
		if err != nil {
			return nil, 0, err
		}

		if hasDim {
			dimension, err := domain.DimensionFromName(attributeOrDimension)
			if err != nil {
				return nil, 0, fmt.Errorf("could not get attribute or dimension for GetDataBuffer: %s", attributeOrDimension)
			}

			datatype, err = dimension.Type()
			if err != nil {
				return nil, 0, fmt.Errorf("could not get dimensionType for GetDataBuffer: %s", attributeOrDimension)
			}
		} else if hasAttr {
			attribute, err := schema.AttributeFromName(attributeOrDimension)
			if err != nil {
				return nil, 0, fmt.Errorf("could not get attribute %s for GetDataBuffer", attributeOrDimension)
			}

			datatype, err = attribute.Type()
			if err != nil {
				return nil, 0, fmt.Errorf("could not get attributeType for GetDataBuffer: %s", attributeOrDimension)
			}
		} else if hasDimLabel {
			datatype, err = q.getDimensionLabelDataType(attributeOrDimension)
			if err != nil {
				return nil, 0, fmt.Errorf("could not get dimension label type for getDataBufferAndSize: %s",
					attributeOrDimension)
			}
		} else {
			return nil, 0, fmt.Errorf("error in getDataBufferAndSize for %s: "+
				"Attribute/dimension/label does not exist", attributeOrDimension)
		}
	}

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	var ret C.int32_t
	var cbufferSize *C.uint64_t
	var cbuffer unsafe.Pointer
	var buffer interface{}

	ret = C.tiledb_query_get_data_buffer(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), cAttributeOrDimension, &cbuffer, &cbufferSize)
	runtime.KeepAlive(q)
	// cbuffer and cbufferSize are in Go-owned memory and don't need a KeepAlive.
	if ret != C.TILEDB_OK {
		return nil, 0, fmt.Errorf("error getting tiledb query data buffer for %s: %w", attributeOrDimension, q.context.LastError())
	}

	var dataNumElements uint64
	if cbufferSize != nil {
		dataNumElements = uint64(*cbufferSize) / datatype.Size()
	}
	if cbuffer == nil {
		return nil, dataNumElements, nil
	}

	switch datatype {
	case TILEDB_INT8:
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]int8)(cbuffer)[:length:length]

	case TILEDB_INT16:
		length := (*cbufferSize) / C.sizeof_int16_t
		buffer = (*[1 << 46]int16)(cbuffer)[:length:length]

	case TILEDB_INT32:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]int32)(cbuffer)[:length:length]

	case TILEDB_INT64, TILEDB_DATETIME_YEAR, TILEDB_DATETIME_MONTH, TILEDB_DATETIME_WEEK, TILEDB_DATETIME_DAY, TILEDB_DATETIME_HR, TILEDB_DATETIME_MIN, TILEDB_DATETIME_SEC, TILEDB_DATETIME_MS, TILEDB_DATETIME_US, TILEDB_DATETIME_NS, TILEDB_DATETIME_PS, TILEDB_DATETIME_FS, TILEDB_DATETIME_AS, TILEDB_TIME_HR, TILEDB_TIME_MIN, TILEDB_TIME_SEC, TILEDB_TIME_MS, TILEDB_TIME_US, TILEDB_TIME_NS, TILEDB_TIME_PS, TILEDB_TIME_FS, TILEDB_TIME_AS:
		length := (*cbufferSize) / C.sizeof_int64_t
		buffer = (*[1 << 46]int64)(cbuffer)[:length:length]

	case TILEDB_UINT8, TILEDB_BLOB, TILEDB_GEOM_WKB, TILEDB_GEOM_WKT:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_UINT16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_UINT32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_UINT64:
		length := (*cbufferSize) / C.sizeof_uint64_t
		buffer = (*[1 << 46]uint64)(cbuffer)[:length:length]

	case TILEDB_FLOAT32:
		length := (*cbufferSize) / C.sizeof_float
		buffer = (*[1 << 46]float32)(cbuffer)[:length:length]

	case TILEDB_FLOAT64:
		length := (*cbufferSize) / C.sizeof_double
		buffer = (*[1 << 46]float64)(cbuffer)[:length:length]

	case TILEDB_CHAR:
		length := (*cbufferSize) / C.sizeof_char
		buffer = (*[1 << 46]byte)(cbuffer)[:length:length]

	case TILEDB_STRING_ASCII:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF8:
		length := (*cbufferSize) / C.sizeof_uint8_t
		buffer = (*[1 << 46]uint8)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF16:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UTF32:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS2:
		length := (*cbufferSize) / C.sizeof_uint16_t
		buffer = (*[1 << 46]uint16)(cbuffer)[:length:length]

	case TILEDB_STRING_UCS4:
		length := (*cbufferSize) / C.sizeof_uint32_t
		buffer = (*[1 << 46]uint32)(cbuffer)[:length:length]

	case TILEDB_ANY:
		length := (*cbufferSize) / C.sizeof_int32_t
		buffer = (*[1 << 46]C.int8_t)(cbuffer)[:length:length]

	case TILEDB_BOOL:
		length := (*cbufferSize) / C.sizeof_int8_t
		buffer = (*[1 << 46]bool)(cbuffer)[:length:length]

	default:
		return nil, 0, fmt.Errorf("unrecognized attribute type: %d", datatype)
	}

	return buffer, dataNumElements, nil
}

// SetValidityBufferUnsafe sets the validity buffer for nullable attribute/dimension.
// This takes an unsafe pointer which is passed straight to tiledb c_api for advanced usage.
func (q *Query) SetValidityBufferUnsafe(attribute string, buffer unsafe.Pointer, bufferSize uint64) (*uint64, error) {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	q.tiledbQuery.Pin(buffer)
	q.tiledbQuery.Pin(&bufferSize)

	ret := C.tiledb_query_set_validity_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttribute,
		(*C.uint8_t)(buffer),
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))
	runtime.KeepAlive(q)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query validity buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attribute, 2, &bufferSize)

	return &bufferSize, nil
}

// SetValidityBuffer sets the validity buffer for nullable attribute/dimension.
func (q *Query) SetValidityBuffer(attributeOrDimension string, buffer []uint8) (*uint64, error) {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	cbuffer := unsafe.Pointer(&buffer[0])
	q.tiledbQuery.Pin(cbuffer)

	bufferSize := uint64(len(buffer)) * bytesizes.Uint8
	if bufferSize == uint64(0) {
		return nil, errors.New("validity slice has no length, validity slices are required to be initialized before reading or writing")
	}
	q.tiledbQuery.Pin(&bufferSize)

	ret := C.tiledb_query_set_validity_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeOrDimension,
		(*C.uint8_t)(cbuffer),
		(*C.uint64_t)(unsafe.Pointer(&bufferSize)))
	runtime.KeepAlive(q)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query validity buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attributeOrDimension, 2, &bufferSize)

	return &bufferSize, nil
}

// GetValidityBuffer retrieves the validity buffer for a nullable attribute/dimension.
func (q *Query) GetValidityBuffer(attributeOrDimension string) ([]uint8, error) {
	buf, _, err := q.getValidityBufferAndSize(attributeOrDimension)
	return buf, err
}

// GetExpectedValidityBufferLength retrieves the size of the validity buffer for a nullable attribute/dimension.
// This is equivalent to calling GetValidityBuffer and taking the length of the returned buffer except
// in the case of a deserialized read query where GetValidityBuffer returns nil. Serialization of read queries serializes
// only lengths not buffers. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) GetExpectedValidityBufferLength(attributeOrDimension string) (uint64, error) {
	_, n, err := q.getValidityBufferAndSize(attributeOrDimension)
	return n, err
}

// getValidityBufferAndSize uses tiledb.get_query_validity_buffer to retrieve the validity buffer and its size for a nullable attribute/dimension
// The returned length is equal to the size of the buffer except in the case of a deserialized read query.
// Serialization of read queries serializes only lengths not buffers thus the methods returns a nil buffer and
// a non zero length. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) getValidityBufferAndSize(attributeOrDimension string) ([]uint8, uint64, error) {
	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var cvalidityByteMapSize *C.uint64_t
	var cvalidityByteMap *C.uint8_t

	ret := C.tiledb_query_get_validity_buffer(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), cattributeNameOrDimension, &cvalidityByteMap, &cvalidityByteMapSize)
	runtime.KeepAlive(q)
	// cvalidityByteMapSize and cvalidityByteMap are in Go-owned memory and do not need a KeepAlive.
	if ret != C.TILEDB_OK {
		return nil, 0, fmt.Errorf("error getting tiledb query validity buffer for %s: %w", attributeOrDimension, q.context.LastError())
	}

	var validityNumElements uint64
	if cvalidityByteMapSize == nil {
		validityNumElements = 0
	} else {
		validityNumElements = uint64(*cvalidityByteMapSize) / TILEDB_UINT8.Size()
	}

	if cvalidityByteMap == nil {
		return nil, validityNumElements, nil
	}

	validityByteMapLength := *cvalidityByteMapSize / C.sizeof_uint8_t
	validities := (*[1 << 46]uint8)(unsafe.Pointer(cvalidityByteMap))[:validityByteMapLength:validityByteMapLength]

	return validities, validityNumElements, nil
}

// SetOffsetsBufferUnsafe sets the offset buffer for a var-sized attribute/dimension.
// This takes an unsafe pointer which is passed straight to tiledb c_api for advanced usage.
func (q *Query) SetOffsetsBufferUnsafe(attribute string, offset unsafe.Pointer, offsetSize uint64) (*uint64, error) {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	cAttribute := C.CString(attribute)
	defer C.free(unsafe.Pointer(cAttribute))

	q.tiledbQuery.Pin(offset)
	q.tiledbQuery.Pin(&offsetSize)

	ret := C.tiledb_query_set_offsets_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttribute,
		(*C.uint64_t)(offset),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)))
	runtime.KeepAlive(q)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query offsets buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attribute, 0, &offsetSize)

	return &offsetSize, nil
}

// SetOffsetsBuffer sets the offset buffer for a var-sized attribute/dimension.
func (q *Query) SetOffsetsBuffer(attributeOrDimension string, offset []uint64) (*uint64, error) {
	q.bufferMutex.Lock()
	defer q.bufferMutex.Unlock()

	cAttributeOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cAttributeOrDimension))

	cbuffer := unsafe.Pointer(&offset[0])
	q.tiledbQuery.Pin(cbuffer)

	offsetSize := uint64(len(offset)) * bytesizes.Uint64
	if offsetSize == uint64(0) {
		return nil, errors.New("offset slice has no length, offset slices are required to be initialized before reading or writing")
	}
	q.tiledbQuery.Pin(&offsetSize)

	ret := C.tiledb_query_set_offsets_buffer(
		q.context.tiledbContext.Get(),
		q.tiledbQuery.Get(),
		cAttributeOrDimension,
		(*C.uint64_t)(unsafe.Pointer(&offset[0])),
		(*C.uint64_t)(unsafe.Pointer(&offsetSize)))
	runtime.KeepAlive(q)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error setting query offsets buffer: %w", q.context.LastError())
	}

	q.setResultBufferPointer(attributeOrDimension, 0, &offsetSize)

	return &offsetSize, nil
}

// GetOffsetsBuffer retrieves the offset buffer for a var-sized attribute/dimension.
func (q *Query) GetOffsetsBuffer(attributeOrDimension string) ([]uint64, error) {
	buf, _, err := q.getOffsetsBufferAndSize(attributeOrDimension)
	return buf, err
}

// GetExpectedOffsetsBufferLength retrieves the size of the offset buffer for a var-sized attribute/dimension.
// This is equivalent to calling GetOffsetsBuffer and taking the length of the returned buffer except
// in the case of a deserialized read query where GetOffsetsBuffer returns nil. Serialization of read queries serializes
// only lengths not buffers. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) GetExpectedOffsetsBufferLength(attributeOrDimension string) (uint64, error) {
	_, n, err := q.getOffsetsBufferAndSize(attributeOrDimension)
	return n, err
}

// getOffsetsBufferAndSize uses tiledb.get_query_offsets_buffer to retrieve the size of the offsets buffer for a var-sized attribute/dimension
// The returned length is equal to the size of the buffer except in the case of a deserialized read query.
// Serialization of read queries serializes only lengths not buffers thus the methods returns a nil buffer and
// a non zero length. The caller should use this method to get the size and allocate a buffer for the read query.
func (q *Query) getOffsetsBufferAndSize(attributeOrDimension string) ([]uint64, uint64, error) {
	cattributeNameOrDimension := C.CString(attributeOrDimension)
	defer C.free(unsafe.Pointer(cattributeNameOrDimension))

	var coffsetsSize *C.uint64_t
	var coffsets *C.uint64_t

	ret := C.tiledb_query_get_offsets_buffer(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), cattributeNameOrDimension, &coffsets, &coffsetsSize)
	runtime.KeepAlive(q)
	// coffsetsSize and coffsets point to Go-owned memory and do not need a KeepAlive
	if ret != C.TILEDB_OK {
		return nil, 0, fmt.Errorf("error getting tiledb query offset buffer for %s: %w", attributeOrDimension, q.context.LastError())
	}

	var offsetNumElements uint64
	if coffsetsSize == nil {
		offsetNumElements = 0
	} else {
		offsetNumElements = uint64(*coffsetsSize) / TILEDB_UINT64.Size()
	}

	if coffsets == nil {
		return nil, offsetNumElements, nil
	}

	offsetsLength := *coffsetsSize / C.sizeof_uint64_t
	offsets := (*[1 << 46]uint64)(unsafe.Pointer(coffsets))[:offsetsLength:offsetsLength]

	return offsets, offsetNumElements, nil
}

// SetSubarray sets the subarray for the query.
func (q *Query) SetSubarray(sa *Subarray) error {
	ret := C.tiledb_query_set_subarray_t(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), sa.subarray.Get())
	runtime.KeepAlive(q)
	runtime.KeepAlive(sa)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting tiledb query subarray: %w", q.context.LastError())
	}
	return nil
}

// GetSubarray gets the subarray set on the query.
func (q *Query) GetSubarray() (*Subarray, error) {
	var sa *C.tiledb_subarray_t

	ret := C.tiledb_query_get_subarray_t(q.context.tiledbContext.Get(), q.tiledbQuery.Get(), &sa)
	runtime.KeepAlive(q)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting tiledb query subarray: %w", q.context.LastError())
	}

	return newSubarrayFromHandle(q.context, q.array, newSubarrayHandle(sa)), nil
}
