package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

/*
FragmentInfo struct representing a TileDB fragment info object.

A FragmentInfo object contains information about fragnents of an array that
can be queried using methods taht have receiver type of *FragmentInfo
*/
type FragmentInfo struct {
	tiledbFragmentInfo *C.tiledb_fragment_info_t
	context            *Context
	uri                string
	array              *Array
	config             *Config
}

// NewFragmentInfo allocates a new fragment info for a given array and fetches all
// the fragment information for that array.
func NewFragmentInfo(tdbCtx *Context, uri string) (*FragmentInfo, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	fI := FragmentInfo{context: tdbCtx, uri: uri}
	ret := C.tiledb_fragment_info_alloc(fI.context.tiledbContext.Get(),
		curi, &fI.tiledbFragmentInfo)
	runtime.KeepAlive(tdbCtx)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error creating tiledb fragment info: %w", fI.context.LastError())
	}
	freeOnGC(&fI)

	return &fI, nil
}

// Free releases the internal TileDB core data that was allocated on the C heap.
// It is automatically called when this object is garbage collected, but can be
// called earlier to manually release memory if needed. Free is idempotent and
// can safely be called many times on the same object; if it has already
// been freed, it will not be freed again.
func (fI *FragmentInfo) Free() {
	if fI.tiledbFragmentInfo != nil {
		C.tiledb_fragment_info_free(&fI.tiledbFragmentInfo)
	}
}

// Context exposes the internal TileDB context used to initialize the fragment info.
func (fI *FragmentInfo) Context() *Context {
	return fI.context
}

// Load loads the fragment info.
func (fI *FragmentInfo) Load() error {
	ret := C.tiledb_fragment_info_load(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error loading tiledb fragment info: %w", fI.context.LastError())
	}
	return nil
}

// GetFragmentNum gets the number of fragments.
func (fI *FragmentInfo) GetFragmentNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_fragment_num(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, &cNum)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting number of fragments from fragment info: %w", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetFragmentURI gets a fragment URI.
// fid is the index of the fragment of interest.
func (fI *FragmentInfo) GetFragmentURI(fid uint32) (string, error) {
	var curi *C.char // fI must be kept alive while curi is being accessed.
	C.tiledb_fragment_info_get_fragment_uri(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &curi)
	uri := C.GoString(curi)
	runtime.KeepAlive(fI)
	if uri == "" {
		return uri, fmt.Errorf("error getting URI for fragment %d: uri is empty", fid)
	}
	return uri, nil
}

// GetFragmentSize gets the fragment size in bytes.
func (fI *FragmentInfo) GetFragmentSize(fid uint32) (uint64, error) {
	var cSize C.uint64_t

	ret := C.tiledb_fragment_info_get_fragment_size(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cSize)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting fragment size for fragment %d: %w", fid, fI.context.LastError())
	}

	return uint64(cSize), nil
}

// GetDense checks if a fragment is dense.
func (fI *FragmentInfo) GetDense(fid uint32) (bool, error) {
	var cDense C.int32_t

	ret := C.tiledb_fragment_info_get_dense(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cDense)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error finding if fragment %d is dense: %w", fid, fI.context.LastError())
	}

	return cDense == 1, nil
}

// GetSparse checks if a fragment is sparse.
func (fI *FragmentInfo) GetSparse(fid uint32) (bool, error) {
	var cSparse C.int32_t

	ret := C.tiledb_fragment_info_get_sparse(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cSparse)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error finding if fragment %d is dense: %w", fid, fI.context.LastError())
	}

	return cSparse == 1, nil
}

// GetTimestampRange gets the timestamp range of a fragment.
func (fI *FragmentInfo) GetTimestampRange(fid uint32) (uint64, uint64, error) {
	var cStart C.uint64_t
	var cEnd C.uint64_t

	ret := C.tiledb_fragment_info_get_timestamp_range(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cStart, &cEnd)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("error getting the timestamp range for fragment %d: %w", fid, fI.context.LastError())
	}

	return uint64(cStart), uint64(cEnd), nil
}

func (fI *FragmentInfo) useArrayFromCache() error {
	// Array is already set, use it to get schema
	if fI.array != nil {
		return nil
	}

	// Array containing fragments is set as member to reuse for schema retrieval
	var err error
	fI.array, err = NewArray(fI.context, fI.uri)
	if err != nil {
		return err
	}

	return nil
}

func (fI *FragmentInfo) getNonEmptyDomainSliceFromIndex(did uint32) (
	*Dimension, interface{}, unsafe.Pointer, error) {
	err := fI.useArrayFromCache()
	if err != nil {
		return nil, nil, nil, err
	}

	err = fI.array.Open(TILEDB_READ)
	if err != nil {
		return nil, nil, nil, err
	}

	dimension, tmpDimension, tmpDimensionPtr, err := fI.array.GetNonEmptyDomainSliceFromIndex(uint(did))
	if err != nil {
		return nil, nil, nil, err
	}

	err = fI.array.Close()
	if err != nil {
		return nil, nil, nil, err
	}

	return dimension, tmpDimension, tmpDimensionPtr, nil
}

func (fI *FragmentInfo) getNonEmptyDomainSliceFromName(did string) (
	*Dimension, interface{}, unsafe.Pointer, error) {
	err := fI.useArrayFromCache()
	if err != nil {
		return nil, nil, nil, err
	}

	err = fI.array.Open(TILEDB_READ)
	if err != nil {
		return nil, nil, nil, err
	}

	dimension, tmpDimension, tmpDimensionPtr, err := fI.array.GetNonEmptyDomainSliceFromName(did)
	if err != nil {
		return nil, nil, nil, err
	}

	err = fI.array.Close()
	if err != nil {
		return nil, nil, nil, err
	}

	return dimension, tmpDimension, tmpDimensionPtr, nil
}

// GetNonEmptyDomainFromIndex retrieves the non-empty domain from a given fragment for a given
// dimension index.
// func (fI *FragmentInfo) GetNonEmptyDomainFromIndex
func (fI *FragmentInfo) GetNonEmptyDomainFromIndex(fid uint32, did uint32) (*NonEmptyDomain, error) {
	dimension, tmpDimension, tmpDimensionPtr, err := fI.getNonEmptyDomainSliceFromIndex(did)
	if err != nil {
		return nil, err
	}

	var isEmpty C.int32_t
	ret := C.tiledb_fragment_info_get_non_empty_domain_from_index(
		fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		(C.uint32_t)(did),
		tmpDimensionPtr)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in getting non empty domain from fragment %d for a given dimension index %d: %w", fid, did, fI.context.LastError())
	}

	if isEmpty == 1 {
		return nil, nil
	}
	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, err
	}

	return nonEmptyDomain, nil
}

// GetNonEmptyDomainFromName retrieves the non-empty domain from a given
// fragment for a given dimension name.
// func (fI *FragmentInfo) GetNonEmptyDomainFromName
func (fI *FragmentInfo) GetNonEmptyDomainFromName(fid uint32, did string) (*NonEmptyDomain, error) {
	dimension, tmpDimension, tmpDimensionPtr, err := fI.getNonEmptyDomainSliceFromName(did)
	if err != nil {
		return nil, err
	}

	cDid := C.CString(did)
	defer C.free(unsafe.Pointer(cDid))

	ret := C.tiledb_fragment_info_get_non_empty_domain_from_name(
		fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		cDid,
		tmpDimensionPtr)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in getting non empty domain from fragment %d for a given dimension name %s: %w", fid, did, fI.context.LastError())
	}

	// If at least one domain for a dimension is empty the union of domains is non-empty
	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, tmpDimension)
	if err != nil {
		return nil, err
	}

	return nonEmptyDomain, nil
}

// GetNonEmptyDomainVarSizeFromIndex retrieves the non-empty domain range sizes
// from a fragment for a given dimension index. Applicable to var-sized dimensions.
// func (fI *FragmentInfo) GetNonEmptyDomainVarSizeFromName
func (fI *FragmentInfo) GetNonEmptyDomainVarSizeFromIndex(fid uint32, did uint32) (
	uint64, uint64, error) {
	var cStart C.uint64_t
	var cEnd C.uint64_t

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_index(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), C.uint32_t(did), &cStart, &cEnd)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("error retrieving the non-empty domain range sizes from fragment %d for a given dimension index %d: %w", fid, did, fI.context.LastError())
	}

	return uint64(cStart), uint64(cEnd), nil
}

// GetNonEmptyDomainVarSizeFromName retrieves the non-empty domain range sizes
// from a fragment for a given dimension name. Applicable to var-sized dimensions.
func (fI *FragmentInfo) GetNonEmptyDomainVarSizeFromName(fid uint32, did string) (
	uint64, uint64, error) {
	var cStart C.uint64_t
	var cEnd C.uint64_t
	cDid := C.CString(did)
	defer C.free(unsafe.Pointer(cDid))

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_name(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), cDid, &cStart, &cEnd)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("error retrieving the non-empty domain range sizes from fragment %d for a given dimension name %s: %w", fid, did, fI.context.LastError())
	}

	return uint64(cStart), uint64(cEnd), nil
}

// GetNonEmptyDomainVarFromIndex retrieves the non-empty domain from a fragment
// for a given dimension index. Applicable to var-sized dimensions.
func (fI *FragmentInfo) GetNonEmptyDomainVarFromIndex(fid uint32, did uint32) (*NonEmptyDomain, error) {
	var cStartSize C.uint64_t
	var cEndSize C.uint64_t

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_index(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), C.uint32_t(did), &cStartSize, &cEndSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error retrieving the non-empty domain range sizes from fragment %d for a given dimension index %d: %w", fid, did, fI.context.LastError())
	}

	err := fI.useArrayFromCache()
	if err != nil {
		return nil, err
	}

	err = fI.array.Open(TILEDB_READ)
	if err != nil {
		return nil, err
	}

	schema, err := fI.array.Schema()
	if err != nil {
		return nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}

	dimension, err := domain.DimensionFromIndex(uint(did))
	if err != nil {
		return nil, fmt.Errorf("could not get dimension having index: %d", did)
	}

	dimType, err := dimension.Type()
	if err != nil {
		return nil, err
	}

	bounds := make([]interface{}, 0)

	start, cstart, err := dimType.MakeSlice(uint64(cStartSize))
	if err != nil {
		return nil, err
	}
	bounds = append(bounds, start)

	end, cend, err := dimType.MakeSlice(uint64(cEndSize))
	if err != nil {
		return nil, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_fragment_info_get_non_empty_domain_var_from_index(
		fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		(C.uint32_t)(did),
		cstart,
		cend)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in getting non empty domain for dimension index %d for fragment info %d: %w",
			did, fid, fI.context.LastError())
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, err
	}

	err = fI.array.Close()
	if err != nil {
		return nil, err
	}

	runtime.KeepAlive(fI)
	return nonEmptyDomain, nil
}

// GetNonEmptyDomainVarFromName retrieves the non-empty domain from a fragment
// for a given dimension name. Applicable to var-sized dimensions.
func (fI *FragmentInfo) GetNonEmptyDomainVarFromName(fid uint32, did string) (*NonEmptyDomain, error) {
	var cStartSize C.uint64_t
	var cEndSize C.uint64_t

	cDid := C.CString(did)
	defer C.free(unsafe.Pointer(cDid))

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_name(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), cDid, &cStartSize, &cEndSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error retrieving the non-empty domain range sizes from fragment %d for a given dimension name %s: %w", fid, did, fI.context.LastError())
	}

	err := fI.useArrayFromCache()
	if err != nil {
		return nil, err
	}

	err = fI.array.Open(TILEDB_READ)
	if err != nil {
		return nil, err
	}

	schema, err := fI.array.Schema()
	if err != nil {
		return nil, err
	}

	domain, err := schema.Domain()
	if err != nil {
		return nil, err
	}

	dimension, err := domain.DimensionFromName(did)
	if err != nil {
		return nil, fmt.Errorf("could not get dimension having name: %s", did)
	}

	dimType, err := dimension.Type()
	if err != nil {
		return nil, err
	}

	bounds := make([]interface{}, 0)

	start, cstart, err := dimType.MakeSlice(uint64(cStartSize))
	if err != nil {
		return nil, err
	}
	bounds = append(bounds, start)

	end, cend, err := dimType.MakeSlice(uint64(cEndSize))
	if err != nil {
		return nil, err
	}
	bounds = append(bounds, end)

	ret = C.tiledb_fragment_info_get_non_empty_domain_var_from_name(
		fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		cDid,
		cstart,
		cend)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error in getting non empty domain for dimension name %s for fragment info %d: %w",
			did, fid, fI.context.LastError())
	}

	nonEmptyDomain, err := getNonEmptyDomainForDim(dimension, bounds)
	if err != nil {
		return nil, err
	}

	err = fI.array.Close()
	if err != nil {
		return nil, err
	}

	runtime.KeepAlive(fI)
	return nonEmptyDomain, nil
}

// GetCellNum retrieves the number of cells written to the fragment by the user.
// In the case of sparse fragments, this is the number of non-empty
// cells in the fragment.
// In the case of dense fragments, TileDB may add fill
// values to populate partially populated tiles. Those fill values
// are counted in the returned number of cells. In other words,
// the cell number is derived from the number of *integral* tiles
// written in the file.
func (fI *FragmentInfo) GetCellNum(fid uint32) (uint64, error) {
	var cCellNum C.uint64_t

	ret := C.tiledb_fragment_info_get_cell_num(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cCellNum)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error retrieving number of cells written to the fragment %d by the user: %w", fid, fI.context.LastError())
	}

	return uint64(cCellNum), nil
}

// GetVersion retrieves the format version of a fragment.
func (fI *FragmentInfo) GetVersion(fid uint32) (uint32, error) {
	var cVersion C.uint32_t

	ret := C.tiledb_fragment_info_get_version(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cVersion)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error finding version of fragment %d: %w", fid, fI.context.LastError())
	}

	return uint32(cVersion), nil
}

// HasConsolidatedMetadata checks if a fragment has consolidated metadata.
func (fI *FragmentInfo) HasConsolidatedMetadata(fid uint32) (bool, error) {
	var cHas C.int32_t

	ret := C.tiledb_fragment_info_has_consolidated_metadata(fI.context.tiledbContext.Get(),
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cHas)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("error finding if fragment %d has consolidated metadata: %w", fid, fI.context.LastError())
	}

	return cHas == 1, nil
}

// GetUnconsolidatedMetadataNum gets the number of fragments with unconsolidated metadata.
// func (fI *FragmentInfo) GetUnconsolidatedMetadataNum
func (fI *FragmentInfo) GetUnconsolidatedMetadataNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_unconsolidated_metadata_num(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, &cNum)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting number of fragments with unconsolidated metadata: %w", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetToVacuumNum gets the number of fragments to vacuum.
func (fI *FragmentInfo) GetToVacuumNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_to_vacuum_num(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, &cNum)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("error getting number of fragments to vacuum: %w", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetToVacuumURI gets the URI of the fragment to vacuum with the given index.
// fid is the index of the fragment of interest.
func (fI *FragmentInfo) GetToVacuumURI(fid uint32) (string, error) {
	var curi *C.char
	ret := C.tiledb_fragment_info_get_to_vacuum_uri(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, C.uint32_t(fid), &curi)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error getting URI uri for fragment to vacuum: %w", fI.context.LastError())
	}
	uri := C.GoString(curi)
	if uri == "" {
		return "", fmt.Errorf("error getting URI for fragment %d to vacuum: uri is empty", fid)
	}
	return uri, nil
}

// DumpSTDOUT dumps the fragment info in ASCII format in the selected output.
func (fI *FragmentInfo) DumpSTDOUT() error {
	ret := C.tiledb_fragment_info_dump(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, C.stdout)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping fragment info to stdout: %w", fI.context.LastError())
	}
	return nil
}

// String retrieves the string representation of the FragmentInfo
func (fI *FragmentInfo) String() (string, error) {
	var tdbString *C.tiledb_string_t

	ret := C.tiledb_fragment_info_dump_str(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, &tdbString)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("error dumping fragment info to string: %w", fI.context.LastError())
	}
	defer C.tiledb_string_free(&tdbString)

	dumpStr, err := stringHandleToString(tdbString)
	if err != nil {
		return "", fmt.Errorf("error getting fragment info string: %w", fI.context.LastError())
	}
	return dumpStr, nil
}

// SetConfig sets the fragment config.
func (fI *FragmentInfo) SetConfig(config *Config) error {
	ret := C.tiledb_fragment_info_set_config(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, config.tiledbConfig.Get())
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error setting config on group: %w", fI.context.LastError())
	}
	fI.config = config
	return nil
}

// Config gets the fragment config.
func (fI *FragmentInfo) Config() (*Config, error) {
	var configPtr *C.tiledb_config_t
	ret := C.tiledb_fragment_info_get_config(fI.context.tiledbContext.Get(), fI.tiledbFragmentInfo, &configPtr)
	runtime.KeepAlive(fI)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("error getting config from fragment info: %w", fI.context.LastError())
	}

	return newConfigFromHandle(newConfigHandle(configPtr)), nil
}
