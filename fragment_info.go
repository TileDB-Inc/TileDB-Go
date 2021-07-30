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
	encryptionType     EncryptionType
	encryptionKey      string
}

// NewFragmentInfo alloc a new fragment info for a given array and fetches all
// the fragment information for that array.
func NewFragmentInfo(tdbCtx *Context, uri string) (*FragmentInfo, error) {
	curi := C.CString(uri)
	defer C.free(unsafe.Pointer(curi))
	fI := FragmentInfo{context: tdbCtx, uri: uri}
	ret := C.tiledb_fragment_info_alloc(fI.context.tiledbContext,
		curi, &fI.tiledbFragmentInfo)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error creating tiledb fragment info: %s", fI.context.LastError())
	}

	// Set finalizer for free C pointer on gc
	runtime.SetFinalizer(&fI, func(fragmentInfo *FragmentInfo) {
		fragmentInfo.Free()
	})

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

// Load loads the fragment info.
func (fI *FragmentInfo) Load() error {
	ret := C.tiledb_fragment_info_load(fI.context.tiledbContext, fI.tiledbFragmentInfo)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error loading tiledb fragment info: %s", fI.context.LastError())
	}
	return nil
}

// LoadWithKey loads the fragment info from an encrypted array.
func (fI *FragmentInfo) LoadWithKey(encryptionType EncryptionType, key string) error {
	ckey := unsafe.Pointer(C.CString(key))
	defer C.free(ckey)
	ret := C.tiledb_fragment_info_load_with_key(fI.context.tiledbContext, fI.tiledbFragmentInfo,
		C.tiledb_encryption_type_t(encryptionType), ckey, C.uint32_t(len(key)))
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error loading tiledb fragment info with key: %s", fI.context.LastError())
	}

	fI.encryptionType = encryptionType
	fI.encryptionKey = key

	return nil
}

// GetFragmentNum gets the number of fragments.
func (fI *FragmentInfo) GetFragmentNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_fragment_num(fI.context.tiledbContext, fI.tiledbFragmentInfo, &cNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting number of fragments from fragment info: %s", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetFragmentURI gets a fragment URI.
// fid is the index of the fragment of interest.
func (fI *FragmentInfo) GetFragmentURI(fid uint32) (string, error) {
	var curi *C.char
	C.tiledb_fragment_info_get_fragment_uri(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &curi)
	uri := C.GoString(curi)
	if uri == "" {
		return uri, fmt.Errorf("Error getting URI for fragment %d: uri is empty", fid)
	}
	return uri, nil
}

// GetFragmentSize gets the fragment size in bytes.
func (fI *FragmentInfo) GetFragmentSize(fid uint32) (uint64, error) {
	var cSize C.uint64_t

	ret := C.tiledb_fragment_info_get_fragment_size(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cSize)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting fragment size for fragment %d: %s", fid, fI.context.LastError())
	}

	return uint64(cSize), nil
}

// GetDense checks if a fragment is dense.
func (fI *FragmentInfo) GetDense(fid uint32) (bool, error) {
	var cDense C.int32_t

	ret := C.tiledb_fragment_info_get_dense(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cDense)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error finding if fragment %d is dense: %s", fid, fI.context.LastError())
	}

	return cDense == 1, nil
}

// GetSparse checks if a fragment is sparse.
func (fI *FragmentInfo) GetSparse(fid uint32) (bool, error) {
	var cSparse C.int32_t

	ret := C.tiledb_fragment_info_get_sparse(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cSparse)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error finding if fragment %d is dense: %s", fid, fI.context.LastError())
	}

	return cSparse == 1, nil
}

// GetTimestampRange gets the timestamp range of a fragment.
func (fI *FragmentInfo) GetTimestampRange(fid uint32) (uint64, uint64, error) {
	var cStart C.uint64_t
	var cEnd C.uint64_t

	ret := C.tiledb_fragment_info_get_timestamp_range(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cStart, &cEnd)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error getting the timestamp range for fragment %d: %s", fid, fI.context.LastError())
	}

	return uint64(cStart), uint64(cEnd), nil
}

func (fI *FragmentInfo) useArrayFromCache() error {
	// Array is already set, use it to get schema
	if fI.array != nil {
		return nil
	}

	config, err := NewConfig()
	if err != nil {
		return err
	}

	context, err := NewContext(config)
	if err != nil {
		return err
	}

	// Array containing fragments is set as member to reuse for schema retrieval
	fI.array, err = NewArray(context, fI.uri)
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

	if fI.encryptionKey != "" {
		err = fI.array.OpenWithKey(TILEDB_READ, fI.encryptionType, fI.encryptionKey)
	} else {
		err = fI.array.Open(TILEDB_READ)
	}
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

	if fI.encryptionKey != "" {
		err = fI.array.OpenWithKey(TILEDB_READ, fI.encryptionType, fI.encryptionKey)
	} else {
		err = fI.array.Open(TILEDB_READ)
	}
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
		fI.context.tiledbContext,
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		(C.uint32_t)(did),
		tmpDimensionPtr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in getting non empty domain from fragment %d for a given dimension index %d: %s", fid, did, fI.context.LastError())
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
		fI.context.tiledbContext,
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		cDid,
		tmpDimensionPtr)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in getting non empty domain from fragment %d for a given dimension name %s: %s", fid, did, fI.context.LastError())
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

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_index(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), C.uint32_t(did), &cStart, &cEnd)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error retrieving the non-empty domain range sizes from fragment %d for a given dimension index %d: %s", fid, did, fI.context.LastError())
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

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_name(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), cDid, &cStart, &cEnd)
	if ret != C.TILEDB_OK {
		return 0, 0, fmt.Errorf("Error retrieving the non-empty domain range sizes from fragment %d for a given dimension name %s: %s", fid, did, fI.context.LastError())
	}

	return uint64(cStart), uint64(cEnd), nil
}

// GetNonEmptyDomainVarFromIndex retrieves the non-empty domain from a fragment
// for a given dimension index. Applicable to var-sized dimensions.
func (fI *FragmentInfo) GetNonEmptyDomainVarFromIndex(fid uint32, did uint32) (*NonEmptyDomain, error) {
	var cStartSize C.uint64_t
	var cEndSize C.uint64_t

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_index(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), C.uint32_t(did), &cStartSize, &cEndSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error retrieving the non-empty domain range sizes from fragment %d for a given dimension index %d: %s", fid, did, fI.context.LastError())
	}

	err := fI.useArrayFromCache()
	if err != nil {
		return nil, err
	}

	if fI.encryptionKey != "" {
		err = fI.array.OpenWithKey(TILEDB_READ, fI.encryptionType, fI.encryptionKey)
	} else {
		err = fI.array.Open(TILEDB_READ)
	}
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
		return nil, fmt.Errorf("Could not get dimension having index: %d", did)
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
		fI.context.tiledbContext,
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		(C.uint32_t)(did),
		cstart,
		cend)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in getting non empty domain for dimension index %d for fragment info %d: %s",
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

	return nonEmptyDomain, nil
}

// GetNonEmptyDomainVarFromName retrieves the non-empty domain from a fragment
// for a given dimension name. Applicable to var-sized dimensions.
func (fI *FragmentInfo) GetNonEmptyDomainVarFromName(fid uint32, did string) (*NonEmptyDomain, error) {
	var cStartSize C.uint64_t
	var cEndSize C.uint64_t

	cDid := C.CString(did)
	defer C.free(unsafe.Pointer(cDid))

	ret := C.tiledb_fragment_info_get_non_empty_domain_var_size_from_name(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), cDid, &cStartSize, &cEndSize)
	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error retrieving the non-empty domain range sizes from fragment %d for a given dimension name %s: %s", fid, did, fI.context.LastError())
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
		return nil, fmt.Errorf("Could not get dimension having name: %s", did)
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
		fI.context.tiledbContext,
		fI.tiledbFragmentInfo,
		(C.uint32_t)(fid),
		cDid,
		cstart,
		cend)

	if ret != C.TILEDB_OK {
		return nil, fmt.Errorf("Error in getting non empty domain for dimension name %s for fragment info %d: %s",
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

	ret := C.tiledb_fragment_info_get_cell_num(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cCellNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error retrieving number of cells written to the fragment %d by the user: %s", fid, fI.context.LastError())
	}

	return uint64(cCellNum), nil
}

// GetVersion retrieves the format version of a fragment.
func (fI *FragmentInfo) GetVersion(fid uint32) (uint32, error) {
	var cVersion C.uint32_t

	ret := C.tiledb_fragment_info_get_version(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cVersion)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error finding version of fragment %d: %s", fid, fI.context.LastError())
	}

	return uint32(cVersion), nil
}

// HasConsolidatedMetadata checks if a fragment has consolidated metadata.
func (fI *FragmentInfo) HasConsolidatedMetadata(fid uint32) (bool, error) {
	var cHas C.int32_t

	ret := C.tiledb_fragment_info_has_consolidated_metadata(fI.context.tiledbContext,
		fI.tiledbFragmentInfo, C.uint32_t(fid), &cHas)
	if ret != C.TILEDB_OK {
		return false, fmt.Errorf("Error finding if fragment %d has consolidated metadata: %s", fid, fI.context.LastError())
	}

	return cHas == 1, nil
}

// GetUnconsolidatedMetadataNum gets the number of fragments with unconsolidated metadata.
// func (fI *FragmentInfo) GetUnconsolidatedMetadataNum
func (fI *FragmentInfo) GetUnconsolidatedMetadataNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_unconsolidated_metadata_num(fI.context.tiledbContext, fI.tiledbFragmentInfo, &cNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting number of fragments with unconsolidated metadata: %s", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetToVacuumNum gets the number of fragments to vacuum.
func (fI *FragmentInfo) GetToVacuumNum() (uint32, error) {
	var cNum C.uint32_t

	ret := C.tiledb_fragment_info_get_to_vacuum_num(fI.context.tiledbContext, fI.tiledbFragmentInfo, &cNum)
	if ret != C.TILEDB_OK {
		return 0, fmt.Errorf("Error getting number of fragments to vacuum: %s", fI.context.LastError())
	}

	return uint32(cNum), nil
}

// GetToVacuumURI gets the URI of the fragment to vacuum with the given index.
// fid is the index of the fragment of interest.
func (fI *FragmentInfo) GetToVacuumURI(fid uint32) (string, error) {
	var curi *C.char
	ret := C.tiledb_fragment_info_get_to_vacuum_uri(fI.context.tiledbContext, fI.tiledbFragmentInfo, C.uint32_t(fid), &curi)
	if ret != C.TILEDB_OK {
		return "", fmt.Errorf("Error getting URI uri for fragment to vacuum: %s", fI.context.LastError())
	}
	uri := C.GoString(curi)
	if uri == "" {
		return "", fmt.Errorf("Error getting URI for fragment %d to vacuum: uri is empty", fid)
	}
	return uri, nil
}

// DumpSTDOUT dumps the fragment info in ASCII format in the selected output.
func (fI *FragmentInfo) DumpSTDOUT() error {
	ret := C.tiledb_fragment_info_dump(fI.context.tiledbContext, fI.tiledbFragmentInfo, C.stdout)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("Error dumping fragment info to stdout: %s", fI.context.LastError())
	}
	return nil
}
