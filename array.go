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
	"runtime"
	"time"
	"unsafe"
)

type arrayHandle struct{ *capiHandle }

func freeCapiArray(c unsafe.Pointer) { C.tiledb_array_free((**C.tiledb_array_t)(unsafe.Pointer(&c))) }

func newArrayHandle(ptr *C.tiledb_array_t) arrayHandle {
	return arrayHandle{newCapiHandle(unsafe.Pointer(ptr), freeCapiArray)}
}

func (x arrayHandle) Get() *C.tiledb_array_t {
	return (*C.tiledb_array_t)(x.capiHandle.Get())
}

/*
Array struct representing a TileDB array object.

An Array object represents array data in TileDB at some persisted location,
e.g. on disk, in an S3 bucket, etc. Once an array has been opened for reading
or writing, interact with the data through Query objects.
*/
type Array struct {
	tiledbArray arrayHandle
	context     *Context
	uri         string
}

// ArrayMetadata defines metadata for the array
type ArrayMetadata struct {
	Key      string
	KeyLen   uint32
	Datatype Datatype
	ValueNum uint
	Value    interface{}
}

// MarshalJSON implements the Marshaler interface for ArrayMetadata.
func (a ArrayMetadata) MarshalJSON() ([]byte, error) {
	switch v := a.Value.(type) {
	case []byte:
		return json.Marshal(string(v))
	default:
		return json.Marshal(v)
	}
}

// NonEmptyDomain contains the non empty dimension bounds and dimension name
type NonEmptyDomain struct {
	DimensionName string
	Bounds        interface{}
}

func newArrayFromHandle(tdbCtx *Context, arrayHandle arrayHandle) *Array {
	return &Array{context: tdbCtx, tiledbArray: arrayHandle}
}

// ConsolidateArray consolidates the fragments of an array into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
func ConsolidateArray(tdbCtx *Context, uri string, config *Config) error {
	if config == nil {
		return errors.New("Config must not be nil for Consolidate")
	}

	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_consolidate(tdbCtx.tiledbContext.Get(), curi, config.tiledbConfig.Get())
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error consolidating tiledb array: %w", tdbCtx.LastError())
	}

	runtime.KeepAlive(config)
	return nil
}

// CreateArray creates a new TileDB array given a context, URI and schema.
func CreateArray(tdbCtx *Context, uri string, arraySchema *ArraySchema) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_create(tdbCtx.tiledbContext.Get(), curi, arraySchema.tiledbArraySchema.Get())
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(arraySchema)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error creating tiledb array: %w", tdbCtx.LastError())
	}
	return nil
}

// VacuumArray cleans up an array, such as consolidated fragments and array metadata.
func VacuumArray(tdbCtx *Context, uri string, config *Config) error {
	if config == nil {
		return errors.New("Config must not be nil for Vacuum")
	}

	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	ret := C.tiledb_array_vacuum(tdbCtx.tiledbContext.Get(), curi, config.tiledbConfig.Get())
	runtime.KeepAlive(tdbCtx)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error vacuuming tiledb array: %w", tdbCtx.LastError())
	}

	runtime.KeepAlive(config)
	return nil
}

// NewArray allocates a new array.
// If the provided Context is nil, a default context is allocated and used.
func NewArray(tdbCtx *Context, uri string) (*Array, error) {
	if tdbCtx == nil {
		newCtx, err := NewContext(nil)
		if err != nil {
			return nil, err
		}
		tdbCtx = newCtx
	}
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	var arrayPtr *C.tiledb_array_t
	ret := C.tiledb_array_alloc(tdbCtx.tiledbContext.Get(), curi, &arrayPtr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb array: %w", tdbCtx.LastError())
	}
	array := newArrayFromHandle(tdbCtx, newArrayHandle(arrayPtr))
	array.uri = uri

	return array, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (a *Array) Free() {
	a.tiledbArray.Free()
}

// Context returns the TileDB context used to initialize the array.
func (a *Array) Context() *Context {
	return a.context
}

// ArrayOpenOptions defines the flexible parameters in which arrays can be opened with.
type ArrayOpenOption func(tdbArray *Array) error

// WithEndTime sets the subsequent Open call to use the given time
// as its end timestamp. If "end" is the zero value, does nothing.
func WithEndTime(end time.Time) ArrayOpenOption {
	if end.IsZero() {
		return func(*Array) error { return nil }
	}
	return WithEndTimestamp(uint64(end.UnixMilli()))
}

// WithStartTime sets the subsequent Open call to use the given time
// as its start timestamp. If "start" is the zero value, does nothing.
func WithStartTime(start time.Time) ArrayOpenOption {
	if start.IsZero() {
		return func(*Array) error { return nil }
	}
	return WithStartTimestamp(uint64(start.UnixMilli()))
}

// WithEndTimestamp sets the subsequent Open call to use the end_timestamp of the passed value.
func WithEndTimestamp(endTimestamp uint64) ArrayOpenOption {
	return func(tdbArray *Array) error {
		ret := C.tiledb_array_set_open_timestamp_end(tdbArray.context.tiledbContext.Get(), tdbArray.tiledbArray.Get(), C.uint64_t(endTimestamp))
		runtime.KeepAlive(tdbArray)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error setting end timestamp option: %w", tdbArray.context.LastError())
		}
		return nil
	}
}

// WithStartTimestamp sets the subsequent Open call to use the start_timestamp of the passed value.
func WithStartTimestamp(startTimestamp uint64) ArrayOpenOption {
	return func(tdbArray *Array) error {
		ret := C.tiledb_array_set_open_timestamp_start(tdbArray.context.tiledbContext.Get(), tdbArray.tiledbArray.Get(), C.uint64_t(startTimestamp))
		runtime.KeepAlive(tdbArray)
		if ret != C.TILEDB_OK {
			return fmt.Errorf("error setting start timestamp option: %w", tdbArray.context.LastError())
		}
		return nil
	}
}

/*
OpenWithOptions opens the array with options. The array is opened using a query type as input.
This is to indicate that queries created for this Array object will inherit
the query type. In other words, Array objects are opened to receive only one
type of query. They can always be closed and be re-opened with another query
type. Also there may be many different Array objects created and opened with
different query types. For instance, one may create and open an array object
array_read for reads and another one array_write for writes, and interleave
creation and submission of queries for both these array objects.
*/
func (a *Array) OpenWithOptions(queryType QueryType, opts ...ArrayOpenOption) error {
	for _, opt := range opts {
		if err := opt(a); err != nil {
			return err
		}
	}

	ret := C.tiledb_array_open(a.context.tiledbContext.Get(), a.tiledbArray.Get(), C.tiledb_query_type_t(queryType))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error opening tiledb array for querying: %w", a.context.LastError())
	}
	return nil
}

/*
Open the array. The array is opened using a query type as input.
This is to indicate that queries created for this Array object will inherit
the query type. In other words, Array objects are opened to receive only one
type of queries. They can always be closed and be re-opened with another query
type. Also there may be many different Array objects created and opened with
different query types. For instance, one may create and open an array object
array_read for reads and another one array_write for writes, and interleave
creation and submission of queries for both these array objects.
*/
func (a *Array) Open(queryType QueryType) error {
	ret := C.tiledb_array_open(a.context.tiledbContext.Get(), a.tiledbArray.Get(), C.tiledb_query_type_t(queryType))
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error opening tiledb array for querying: %w", a.context.LastError())
	}
	return nil
}

/*
Reopen the array (the array must be already open). This is useful when the
array got updated after it got opened and the Array object got created.
To sync-up with the updates, the user must either close the array and open
with open(), or just use reopen() without closing. This function will be
generally faster than the former alternative.
*/
func (a *Array) Reopen() error {
	ret := C.tiledb_array_reopen(a.context.tiledbContext.Get(), a.tiledbArray.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error reopening tiledb array for querying: %w", a.context.LastError())
	}
	return nil
}

// Close closes a tiledb array. This is automatically called on garbage collection.
func (a *Array) Close() error {
	ret := C.tiledb_array_close(a.context.tiledbContext.Get(), a.tiledbArray.Get())
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error closing tiledb array for querying: %w", a.context.LastError())
	}
	return nil
}

// Create creates a new TileDB array given an input schema.
// Deprecated: Use CreateArray instead.
func (a *Array) Create(arraySchema *ArraySchema) error {
	return CreateArray(a.context, a.uri, arraySchema)
}

// Consolidate consolidates the fragments of the array into a single fragment.
// You must first finalize all queries to the array before consolidation can
// begin (as consolidation temporarily acquires an exclusive lock on the array).
// Deprecated: Use ConsolidateArray instead.
func (a *Array) Consolidate(config *Config) error {
	return ConsolidateArray(a.context, a.uri, config)
}

// Vacuum cleans up the array, such as consolidated fragments and array metadata.
// Deprecated: Use VacuumArray instead.
func (a *Array) Vacuum(config *Config) error {
	return VacuumArray(a.context, a.uri, config)
}

// Schema returns the ArraySchema for the array.
func (a *Array) Schema() (*ArraySchema, error) {
	var arraySchemaPtr *C.tiledb_array_schema_t
	ret := C.tiledb_array_get_schema(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &arraySchemaPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting schema for tiledb array: %w", a.context.LastError())
	}
	return newArraySchemaFromHandle(a.context, newArraySchemaHandle(arraySchemaPtr)), nil
}

// QueryType returns the current query type of an open array.
func (a *Array) QueryType() (QueryType, error) {
	var queryType C.tiledb_query_type_t
	ret := C.tiledb_array_get_query_type(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &queryType)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return -1, fmt.Errorf("error getting QueryType for tiledb array: %w", a.context.LastError())
	}
	return QueryType(queryType), nil
}

// OpenStartTime returns the current start_timestamp of an open array,
// converted to a UTC time.Time.
func (a *Array) OpenStartTime() (time.Time, error) {
	ts, err := a.OpenStartTimestamp()
	if err != nil {
		return time.Time{}, err
	}
	return millisToTime(ts), nil
}

// OpenEndTime returns the current end_timestamp of an open array,
// converted to a UTC time.Time.
func (a *Array) OpenEndTime() (time.Time, error) {
	ts, err := a.OpenEndTimestamp()
	if err != nil {
		return time.Time{}, err
	}
	return millisToTime(ts), nil
}

func millisToTime(epochMillis uint64) time.Time {
	secs, millis := int64(epochMillis/1000), int64(epochMillis%1000)
	return time.Unix(secs, millis*1_000_000).UTC()
}

// OpenStartTimestamp returns the current start_timestamp value of an open array.
func (a *Array) OpenStartTimestamp() (uint64, error) {
	var start C.uint64_t
	ret := C.tiledb_array_get_open_timestamp_start(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &start)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting start timestamp for tiledb array: %w", a.context.LastError())
	}
	return uint64(start), nil
}

// OpenEndTimestamp returns the current end_timestamp value of an open array.
func (a *Array) OpenEndTimestamp() (uint64, error) {
	var end C.uint64_t
	ret := C.tiledb_array_get_open_timestamp_end(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &end)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting end timestamp for tiledb array: %w", a.context.LastError())
	}
	return uint64(end), nil
}

// getNonEmptyDomainForDim creates a NonEmptyDomain from a generic dimension-typed slice.
func getNonEmptyDomainForDim(dimension *Dimension, bounds interface{}) (*NonEmptyDomain, error) {
	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, err
	}

	name, err := dimension.Name()
	if err != nil {
		return nil, err
	}
	switch ds := bounds.(type) {
	case []int8:
		return makeNonEmptyDomain(name, ds)
	case []int16:
		return makeNonEmptyDomain(name, ds)
	case []int32:
		return makeNonEmptyDomain(name, ds)
	case []int64:
		return makeNonEmptyDomain(name, ds)
	case []uint8:
		return makeNonEmptyDomain(name, ds)
	case []uint16:
		return makeNonEmptyDomain(name, ds)
	case []uint32:
		return makeNonEmptyDomain(name, ds)
	case []uint64:
		return makeNonEmptyDomain(name, ds)
	case []float32:
		return makeNonEmptyDomain(name, ds)
	case []float64:
		return makeNonEmptyDomain(name, ds)
	case []bool:
		return makeNonEmptyDomain(name, ds)
	case []any:
		if dimensionType != TILEDB_STRING_ASCII {
			return nil, fmt.Errorf(
				"type mismatch between non-empty domain type (%T) and dimension type (%v); expected %v",
				ds[0], dimensionType, TILEDB_STRING_ASCII,
			)
		}
		lo, hi := ds[0].([]byte), ds[1].([]byte)
		return &NonEmptyDomain{DimensionName: name, Bounds: []string{string(lo), string(hi)}}, nil
	}
	return nil, fmt.Errorf(
		"error creating nonempty domain: unknown data type (slice %T; type %v)",
		bounds, dimensionType,
	)
}

func makeNonEmptyDomain[T any](name string, bounds []T) (*NonEmptyDomain, error) {
	return &NonEmptyDomain{DimensionName: name, Bounds: []T{bounds[0], bounds[1]}}, nil
}

// NonEmptyDomain retrieves the non-empty domain from an array.
// This returns the bounding coordinates for each dimension.
func (a *Array) NonEmptyDomain() ([]NonEmptyDomain, bool, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}
	defer domain.Free()

	ndims, err := domain.NDim()
	if err != nil {
		return nil, false, err
	}

	isDomainEmpty := true
	nonEmptyDomains := make([]NonEmptyDomain, 0)
	for dimIdx := uint(0); dimIdx < ndims; dimIdx++ {
		// Wrapped in a function so `dimension` will be cleaned up with defer each time the function completes.
		err := func() error {
			dimension, err := domain.DimensionFromIndex(dimIdx)
			if err != nil {
				return err
			}
			defer dimension.Free()

			dimensionType, err := dimension.Type()
			if err != nil {
				return err
			}

			tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
			if err != nil {
				return err
			}

			var isEmpty C.int32_t
			ret := C.tiledb_array_get_non_empty_domain_from_index(
				a.context.tiledbContext.Get(),
				a.tiledbArray.Get(),
				(C.uint32_t)(dimIdx),
				tmpDimensionPtr, &isEmpty)
			runtime.KeepAlive(a)
			if ret != C.TILEDB_OK {
				return fmt.Errorf("error in getting non empty domain for dimension: %w", a.context.LastError())
			}

			if isEmpty == 1 {
				return nil
			}

			// If at least one domain for a dimension is empty the union of domains is non-empty
			isDomainEmpty = false
			nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
			if err != nil {
				return err
			}
			nonEmptyDomains = append(nonEmptyDomains, *nonEmptyDomain)

			return nil
		}()

		if err != nil {
			return nil, false, err
		}
	}

	if isDomainEmpty {
		return nil, isDomainEmpty, nil
	}

	return nonEmptyDomains, isDomainEmpty, nil
}

// NonEmptyDomainMap returns a map[string]interface{} where key is the
// dimension name and value is the non empty domain for the given dimension or
// the empty interface. It covers both var-sized and non-var-sized dimensions.
func (a *Array) NonEmptyDomainMap() (map[string]interface{}, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}

	ndims, err := domain.NDim()
	if err != nil {
		return nil, err
	}

	nonEmptyDomainMap := make(map[string]interface{})
	for dimIdx := uint(0); dimIdx < ndims; dimIdx++ {
		dimension, err := domain.DimensionFromIndex(dimIdx)
		if err != nil {
			return nil, err
		}

		dimensionName, err := dimension.Name()
		if err != nil {
			return nil, err
		}

		dimensionType, err := dimension.Type()
		if err != nil {
			return nil, err
		}

		cellValNum, err := dimension.CellValNum()
		if err != nil {
			return nil, err
		}

		if cellValNum == TILEDB_VAR_NUM {
			nonEmptyDomain, isEmpty, err := a.NonEmptyDomainVarFromName(dimensionName)
			if err != nil {
				return nil, err
			}

			if isEmpty {
				var empty interface{}
				nonEmptyDomainMap[dimensionName] = empty
			} else {
				nonEmptyDomainMap[nonEmptyDomain.DimensionName] = nonEmptyDomain.Bounds
			}

		} else {
			tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
			if err != nil {
				return nil, err
			}

			var isEmpty C.int32_t
			ret := C.tiledb_array_get_non_empty_domain_from_index(
				a.context.tiledbContext.Get(),
				a.tiledbArray.Get(),
				(C.uint32_t)(dimIdx),
				tmpDimensionPtr, &isEmpty)
			runtime.KeepAlive(a)
			if ret != C.TILEDB_OK {
				return nil, fmt.Errorf("error in getting non empty domain for dimension: %w", a.context.LastError())
			}

			if isEmpty == 1 {
				var empty interface{}
				nonEmptyDomainMap[dimensionName] = empty
			} else {
				// If at least one domain for a dimension is empty the union of domains is non-empty
				nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
				if err != nil {
					return nil, err
				}
				nonEmptyDomainMap[nonEmptyDomain.DimensionName] = nonEmptyDomain.Bounds
			}
		}

	}

	return nonEmptyDomainMap, nil
}

// NonEmptyDomainVarFromName retrieves the non-empty domain from an array for a
// given var-sized dimension name. Supports only TILEDB_STRING_ASCII type
// Returns the bounding coordinates for the dimension.
func (a *Array) NonEmptyDomainVarFromName(dimName string) (*NonEmptyDomain, bool, error) {

	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}
	defer domain.Free()

	hasDim, err := domain.HasDimension(dimName)
	if err != nil {
		return nil, false, err
	}

	if !hasDim {
		return nil, false, fmt.Errorf("Dimension: %s was not found in domain", dimName)
	}

	dimension, err := domain.DimensionFromName(dimName)
	if err != nil {
		return nil, false, fmt.Errorf("could not get dimension: %s", dimName)
	}
	defer dimension.Free()

	dimType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var cstartSize C.uint64_t
	var cendSize C.uint64_t

	var isEmpty C.int32_t

	var start interface{}
	var end interface{}
	var cstart unsafe.Pointer
	var cend unsafe.Pointer

	ret := C.tiledb_array_get_non_empty_domain_var_size_from_name(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		cDimName,
		&cstartSize,
		&cendSize,
		&isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain size for dimension %s for array: %w", dimName, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	bounds := make([]interface{}, 0)

	start, cstart, err = dimType.MakeSlice(uint64(cstartSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, start)

	end, cend, err = dimType.MakeSlice(uint64(cendSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_array_get_non_empty_domain_var_from_name(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		cDimName,
		cstart,
		cend,
		&isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain for dimension %s for array: %w", dimName, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// NonEmptyDomainVarFromIndex retrieves the non-empty domain from an array for a
// given var-sized dimension index. Supports only TILEDB_STRING_ASCII type
// Returns the bounding coordinates for the dimension.
func (a *Array) NonEmptyDomainVarFromIndex(dimIdx uint) (*NonEmptyDomain, bool, error) {

	schema, err := a.Schema()
	if err != nil {
		return nil, false, err
	}
	defer schema.Free()

	domain, err := schema.Domain()
	if err != nil {
		return nil, false, err
	}
	defer domain.Free()

	dimension, err := domain.DimensionFromIndex(dimIdx)
	if err != nil {
		return nil, false, fmt.Errorf("could not get dimension having index: %d", dimIdx)
	}
	defer dimension.Free()

	dimType, err := dimension.Type()
	if err != nil {
		return nil, false, err
	}

	var cstartSize C.uint64_t
	var cendSize C.uint64_t

	var isEmpty C.int32_t

	var start interface{}
	var end interface{}
	var cstart unsafe.Pointer
	var cend unsafe.Pointer

	ret := C.tiledb_array_get_non_empty_domain_var_size_from_index(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		(C.uint32_t)(dimIdx),
		&cstartSize,
		&cendSize,
		&isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain size for dimension %d for array: %w", dimIdx, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	bounds := make([]interface{}, 0)

	start, cstart, err = dimType.MakeSlice(uint64(cstartSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, start)

	end, cend, err = dimType.MakeSlice(uint64(cendSize))
	if err != nil {
		return nil, false, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_array_get_non_empty_domain_var_from_index(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		(C.uint32_t)(dimIdx),
		cstart,
		cend,
		&isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain for dimension index %d for array: %w", dimIdx, a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

func (a Array) GetNonEmptyDomainSliceFromIndex(dimIdx uint) (*Dimension, interface{}, unsafe.Pointer, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, nil, nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, nil, err
	}

	dimension, err := domain.DimensionFromIndex(dimIdx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not get dimension: %d", dimIdx)
	}

	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, nil, nil, err
	}

	tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
	if err != nil {
		return nil, nil, nil, err
	}

	return dimension, tmpDimension, tmpDimensionPtr, nil
}

func (a Array) GetNonEmptyDomainSliceFromName(dimName string) (*Dimension, interface{}, unsafe.Pointer, error) {
	schema, err := a.Schema()
	if err != nil {
		return nil, nil, nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, nil, nil, err
	}

	hasDim, err := domain.HasDimension(dimName)
	if err != nil {
		return nil, nil, nil, err
	}

	if !hasDim {
		return nil, nil, nil, fmt.Errorf("dimension: %s was not found in domain", dimName)
	}

	dimension, err := domain.DimensionFromName(dimName)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("could not get dimension: %s", dimName)
	}

	dimensionType, err := dimension.Type()
	if err != nil {
		return nil, nil, nil, err
	}

	tmpDimension, tmpDimensionPtr, err := dimensionType.MakeSlice(uint64(2))
	if err != nil {
		return nil, nil, nil, err
	}

	return dimension, tmpDimension, tmpDimensionPtr, nil
}

// NonEmptyDomainFromIndex retrieves the non-empty domain from an array for a
// given fixed-sized dimension index.
// Returns the bounding coordinates for the dimension.
func (a *Array) NonEmptyDomainFromIndex(dimIdx uint) (*NonEmptyDomain, bool, error) {
	dimension, tmpDimension, tmpDimensionPtr, err := a.GetNonEmptyDomainSliceFromIndex(dimIdx)
	if err != nil {
		return nil, false, err
	}

	var isEmpty C.int32_t
	ret := C.tiledb_array_get_non_empty_domain_from_index(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		(C.uint32_t)(dimIdx),
		tmpDimensionPtr, &isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain for dimension: %w", a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}
	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// NonEmptyDomainFromName retrieves the non-empty domain from an array for a
// given fixed-sized dimension name.
// Returns the bounding coordinates for the dimension.
func (a *Array) NonEmptyDomainFromName(dimName string) (*NonEmptyDomain, bool, error) {
	dimension, tmpDimension, tmpDimensionPtr, err := a.GetNonEmptyDomainSliceFromName(dimName)
	if err != nil {
		return nil, false, err
	}

	cDimName := C.CString(dimName)
	defer C.free(unsafe.Pointer(cDimName))

	var isEmpty C.int32_t
	ret := C.tiledb_array_get_non_empty_domain_from_name(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		cDimName,
		tmpDimensionPtr, &isEmpty)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, false, fmt.Errorf("error in getting non empty domain for dimension: %w", a.context.LastError())
	}

	if isEmpty == 1 {
		return nil, true, nil
	}
	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, false, err
	}

	return nonEmptyDomain, false, nil
}

// URI returns the array's uri.
func (a *Array) URI() (string, error) {
	var curi *C.char // a must be kept alive while curi is being accessed.
	C.tiledb_array_get_uri(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &curi)
	uri := C.GoString(curi)
	runtime.KeepAlive(a)
	if uri == "" {
		return uri, errors.New("error getting URI for array: uri is empty")
	}
	return uri, nil
}

// PutCharMetadata adds char metadata to the array.
func (a *Array) PutCharMetadata(key string, charData string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var datatype Datatype = TILEDB_CHAR

	valueNum := C.uint(len(charData))
	ret := C.tiledb_array_put_metadata(a.context.tiledbContext.Get(), a.tiledbArray.Get(),
		ckey, C.tiledb_datatype_t(datatype), valueNum, unsafe.Pointer(&[]byte(charData)[0]))
	runtime.KeepAlive(a)

	if ret != C.TILEDB_OK {
		return fmt.Errorf("error adding char metadata to array: %w", a.context.LastError())
	}

	return nil
}

// PutMetadata puts a metadata key-value item to an open array. The array must
// be opened in WRITE mode, otherwise the function will error out.
func (a *Array) PutMetadata(key string, value interface{}) error {
	switch value := value.(type) {
	case int:
		return arrayPutScalarMetadata(a, tileDBInt, key, value)
	case []int:
		return arrayPutSliceMetadata(a, tileDBInt, key, value)
	case int8:
		return arrayPutScalarMetadata(a, TILEDB_INT8, key, value)
	case []int8:
		return arrayPutSliceMetadata(a, TILEDB_INT8, key, value)
	case int16:
		return arrayPutScalarMetadata(a, TILEDB_INT16, key, value)
	case []int16:
		return arrayPutSliceMetadata(a, TILEDB_INT16, key, value)
	case int32:
		return arrayPutScalarMetadata(a, TILEDB_INT32, key, value)
	case []int32:
		return arrayPutSliceMetadata(a, TILEDB_INT32, key, value)
	case uint:
		return arrayPutScalarMetadata(a, tileDBUint, key, value)
	case []uint:
		return arrayPutSliceMetadata(a, tileDBUint, key, value)
	case int64:
		return arrayPutScalarMetadata(a, TILEDB_INT64, key, value)
	case []int64:
		return arrayPutSliceMetadata(a, TILEDB_INT64, key, value)
	case uint8:
		return arrayPutScalarMetadata(a, TILEDB_UINT8, key, value)
	case []uint8:
		return arrayPutSliceMetadata(a, TILEDB_UINT8, key, value)
	case uint16:
		return arrayPutScalarMetadata(a, TILEDB_UINT16, key, value)
	case []uint16:
		return arrayPutSliceMetadata(a, TILEDB_UINT16, key, value)
	case uint32:
		return arrayPutScalarMetadata(a, TILEDB_UINT32, key, value)
	case []uint32:
		return arrayPutSliceMetadata(a, TILEDB_UINT32, key, value)
	case uint64:
		return arrayPutScalarMetadata(a, TILEDB_UINT64, key, value)
	case []uint64:
		return arrayPutSliceMetadata(a, TILEDB_UINT64, key, value)
	case float32:
		return arrayPutScalarMetadata(a, TILEDB_FLOAT32, key, value)
	case []float32:
		return arrayPutSliceMetadata(a, TILEDB_FLOAT32, key, value)
	case float64:
		return arrayPutScalarMetadata(a, TILEDB_FLOAT64, key, value)
	case []float64:
		return arrayPutSliceMetadata(a, TILEDB_FLOAT64, key, value)
	case bool:
		return arrayPutScalarMetadata(a, TILEDB_BOOL, key, value)
	case []bool:
		return arrayPutSliceMetadata(a, TILEDB_BOOL, key, value)
	case string:
		valPtr := unsafe.Pointer(C.CString(value))
		defer C.free(valPtr)
		return arrayPutMetadata(a, TILEDB_STRING_UTF8, key, valPtr, len(value))
	}
	return fmt.Errorf("can't write %q metadata: unrecognized value type %T", key, value)
}

func arrayPutSliceMetadata[T scalarType](a *Array, dt Datatype, key string, value []T) error {
	if len(value) == 0 {
		return fmt.Errorf("length of %q metadata %T value must be nonzero", key, value)
	}
	return arrayPutMetadata(a, dt, key, slicePtr(value), len(value))
}

func arrayPutScalarMetadata[T scalarType](a *Array, dt Datatype, key string, value T) error {
	return arrayPutMetadata(a, dt, key, unsafe.Pointer(&value), 1)
}

func arrayPutMetadata(a *Array, dt Datatype, key string, valuePtr unsafe.Pointer, count int) error {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))
	ret := C.tiledb_array_put_metadata(
		a.context.tiledbContext.Get(),
		a.tiledbArray.Get(),
		cKey,
		C.tiledb_datatype_t(dt),
		C.uint(count),
		valuePtr,
	)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("could not add metadata to array: %w", a.context.LastError())
	}
	return nil
}

// DeleteMetadata deletes a metadata key-value item from an open array. The array must
// be opened in WRITE mode, otherwise the function will error out.
func (a *Array) DeleteMetadata(key string) error {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	ret := C.tiledb_array_delete_metadata(a.context.tiledbContext.Get(), a.tiledbArray.Get(), ckey)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deleting metadata from array: %w", a.context.LastError())
	}
	return nil
}

// GetMetadata gets a metadata key-value item from an open array. The array must
// be opened in READ mode, otherwise the function will error out.
func (a *Array) GetMetadata(key string) (Datatype, uint, interface{}, error) {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))

	var cType C.tiledb_datatype_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_array_get_metadata(a.context.tiledbContext.Get(), a.tiledbArray.Get(), ckey, &cType, &cValueNum, &cvalue)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, 0, nil, fmt.Errorf("error getting metadata from array: %w, key: %s", a.context.LastError(), key)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return 0, 0, nil, fmt.Errorf("error getting metadata from array, key: %s does not exist", key)
	}

	datatype := Datatype(cType)
	value, err := datatype.GetValue(valueNum, cvalue)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("error getting metadata from array: %w, key: %s", err, key)
	}

	return datatype, valueNum, value, nil
}

// GetMetadataNum gets then number of metadata items in an open array. The array must
// be opened in READ mode, otherwise the function will error out.
func (a *Array) GetMetadataNum() (uint64, error) {
	var cNum C.uint64_t

	ret := C.tiledb_array_get_metadata_num(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &cNum)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting number of metadata from array: %w", a.context.LastError())
	}

	return uint64(cNum), nil
}

// GetMetadataFromIndex gets a metadata item from an open array using an index.
// The array must be opened in READ mode, otherwise the function will
// error out.
func (a *Array) GetMetadataFromIndex(index uint64) (*ArrayMetadata, error) {
	return a.GetMetadataFromIndexWithValueLimit(index, nil)
}

// GetMetadataFromIndexWithValueLimit gets a metadata item from an open array using an index.
// The array must be opened in READ mode, otherwise the function will
// error out.
// limit parameter limits the number of values returned if string or array
// This is helpful for pushdown of limitting metadata. If nil value is returned
// in full.
func (a *Array) GetMetadataFromIndexWithValueLimit(index uint64, limit *uint) (*ArrayMetadata, error) {
	var cKey *C.char

	var cIndex C.uint64_t = C.uint64_t(index)
	var cType C.tiledb_datatype_t
	var cKeyLen C.uint32_t
	var cValueNum C.uint
	var cvalue unsafe.Pointer

	ret := C.tiledb_array_get_metadata_from_index(a.context.tiledbContext.Get(),
		a.tiledbArray.Get(), cIndex, &cKey, &cKeyLen, &cType, &cValueNum, &cvalue)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting metadata from array: %s, Index: %d", a.context.LastError(), index)
	}

	valueNum := uint(cValueNum)
	if valueNum == 0 {
		return nil, fmt.Errorf("error getting metadata from array, Index: %d does not exist", index)
	}

	datatype := Datatype(cType)
	if limit != nil && valueNum > *limit {
		valueNum = *limit
	}
	value, err := datatype.GetValue(valueNum, cvalue)
	if err != nil {
		return nil, fmt.Errorf("%s, Index: %d", err.Error(), index)
	}

	arrayMetadata := ArrayMetadata{
		Key:      C.GoStringN(cKey, C.int(cKeyLen)),
		KeyLen:   uint32(cKeyLen),
		Datatype: datatype,
		ValueNum: valueNum,
		Value:    value,
	}

	return &arrayMetadata, nil
}

// GetMetadataMap returns a map with the array's metadata, indexed by their key.
func (a *Array) GetMetadataMap() (map[string]*ArrayMetadata, error) {
	return a.GetMetadataMapWithValueLimit(nil)
}

// GetMetadataMapWithValueLimit returns a map with the array's metadata, indexed by their key.
// The limit parameter limits the size of values returned if string or array.
// This is helpful for pushdown of limiting metadata. If nil, value is returned
// in full.
func (a *Array) GetMetadataMapWithValueLimit(limit *uint) (map[string]*ArrayMetadata, error) {
	metadataMap := make(map[string]*ArrayMetadata)

	numOfMetadata, err := a.GetMetadataNum()
	if err != nil {
		return nil, err
	}

	var I uint64
	for I = 0; I < numOfMetadata; I++ {
		arrayMetadata, err := a.GetMetadataFromIndexWithValueLimit(I, limit)
		if err != nil {
			return nil, err
		}
		metadataMap[arrayMetadata.Key] = arrayMetadata
	}

	return metadataMap, nil
}

// SetConfig sets the array config.
func (a *Array) SetConfig(config *Config) error {
	ret := C.tiledb_array_set_config(a.context.tiledbContext.Get(), a.tiledbArray.Get(), config.tiledbConfig.Get())
	runtime.KeepAlive(a)
	runtime.KeepAlive(config)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting config on array: %w", a.context.LastError())
	}

	return nil
}

// Config gets the array config.
func (a *Array) Config() (*Config, error) {
	var configPtr *C.tiledb_config_t
	ret := C.tiledb_array_get_config(a.context.tiledbContext.Get(), a.tiledbArray.Get(), &configPtr)
	runtime.KeepAlive(a)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting config from array: %w", a.context.LastError())
	}

	return newConfigFromHandle(newConfigHandle(configPtr)), nil
}

// DeleteFragments deletes the range of fragments from startTimestamp to endTimestamp.
func DeleteFragments(tdbCtx *Context, uri string, startTimestamp, endTimestamp uint64) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	ret := C.tiledb_array_delete_fragments_v2(tdbCtx.tiledbContext.Get(), curi,
		C.uint64_t(startTimestamp), C.uint64_t(endTimestamp))
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deleting fragments from array: %w", tdbCtx.LastError())
	}

	return nil
}

// DeleteFragmentsList deletes the fragments of the list.
func DeleteFragmentsList(tdbCtx *Context, uri string, fragmentURIs []string) error {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))

	list, freeMemory := cStringArray(fragmentURIs)
	defer freeMemory()

	ret := C.tiledb_array_delete_fragments_list(tdbCtx.tiledbContext.Get(), curi, (**C.char)(slicePtr(list)), C.size_t(len(list)))
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error deleting fragments list from array: %w", tdbCtx.LastError())
	}

	runtime.KeepAlive(list)

	return nil
}
