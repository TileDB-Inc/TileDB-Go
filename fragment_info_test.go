package tiledb

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func writeToArray(t *testing.T, tmpArrayPath string, key string) {
	ctx, err := NewContext(nil)
	assert.Nil(t, err)

	// Prepare some data for the array
	a1 := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	a2 := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}
	a2Off := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Create the query
	array, err := NewArray(ctx, tmpArrayPath)
	assert.Nil(t, err)

	if key != "" {
		err = array.OpenWithKey(TILEDB_WRITE, TILEDB_AES_256_GCM, key)
		assert.Nil(t, err)
	} else {
		err = array.Open(TILEDB_WRITE)
		assert.Nil(t, err)
	}

	query, err := NewQuery(ctx, array)
	assert.Nil(t, err)
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	_, err = query.SetBuffer("a1", a1)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2)
	assert.Nil(t, err)
	assert.Nil(t, err)

	err = query.Submit()
	assert.Nil(t, err)
	err = array.Close()
	assert.Nil(t, err)
}

func TestFragmentInfo(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	// Get array URI
	uri, err := array.URI()
	assert.Nil(t, err)
	assert.Equal(t, "file://"+tmpArrayPath, uri)

	// Close Array
	err = array.Close()
	assert.Nil(t, err)

	array.Free()

	// Create new fragment info struct
	fI, err := NewFragmentInfo(context, uri)
	assert.Nil(t, err)
	assert.NotNil(t, fI)

	// Load fragment info
	err = fI.Load()
	assert.Nil(t, err)

	num, err := fI.GetFragmentNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), num)

	writeToArray(t, tmpArrayPath, "")

	// Load fragment info again
	err = fI.Load()
	assert.Nil(t, err)

	num, err = fI.GetFragmentNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), num)

	fragmentURI, err := fI.GetFragmentURI(0)
	assert.Nil(t, err)
	assert.NotEmpty(t, fragmentURI)

	fragmentSize, err := fI.GetFragmentSize(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2219), fragmentSize)

	isDense, err := fI.GetDense(0)
	assert.Nil(t, err)
	assert.Equal(t, true, isDense)

	isSparse, err := fI.GetSparse(0)
	assert.Nil(t, err)
	assert.Equal(t, false, isSparse)

	t1, t2, err := fI.GetTimestampRange(0)
	assert.Nil(t, err)
	assert.Equal(t, t2, t1)

	nonEmptyDomain, err := fI.GetNonEmptyDomainFromIndex(0, 0)
	assert.Nil(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	nonEmptyDomain, err = fI.GetNonEmptyDomainFromName(0, "dim1")
	assert.Nil(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	cellNum, err := fI.GetCellNum(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), cellNum)

	version, err := fI.GetVersion(0)
	assert.Nil(t, err)
	assert.Equal(t, uint32(7), version)

	hasConsolidatedMetadata, err := fI.HasConsolidatedMetadata(0)
	assert.Nil(t, err)
	assert.Equal(t, false, hasConsolidatedMetadata)

	unconsolidatedMetadataNum, err := fI.GetUnconsolidatedMetadataNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), unconsolidatedMetadataNum)

	toVacuumNum, err := fI.GetToVacuumNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), toVacuumNum)

	_, err = fI.GetToVacuumURI(0)
	assert.NotNil(t, err)

	fI.Free()
}

func TestFragmentInfoEncryption(t *testing.T) {
	key := "unittestunittestunittestunittest"
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	err = array.CreateWithKey(arraySchema, TILEDB_AES_256_GCM, key)
	assert.Nil(t, err)

	// Get array URI
	uri, err := array.URI()
	assert.Nil(t, err)
	assert.Equal(t, "file://"+tmpArrayPath, uri)

	// Close Array
	err = array.Close()
	assert.Nil(t, err)

	array.Free()

	// Create new fragment info struct
	fI, err := NewFragmentInfo(context, uri)
	assert.Nil(t, err)
	assert.NotNil(t, fI)

	// Load fragment info
	err = fI.LoadWithKey(TILEDB_AES_256_GCM, key)
	assert.Nil(t, err)

	num, err := fI.GetFragmentNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), num)

	writeToArray(t, tmpArrayPath, key)

	// Load fragment info again
	err = fI.LoadWithKey(TILEDB_AES_256_GCM, key)
	assert.Nil(t, err)

	num, err = fI.GetFragmentNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), num)

	fragmentURI, err := fI.GetFragmentURI(0)
	assert.Nil(t, err)
	assert.NotEmpty(t, fragmentURI)

	fragmentSize, err := fI.GetFragmentSize(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(4000), fragmentSize)

	isDense, err := fI.GetDense(0)
	assert.Nil(t, err)
	assert.Equal(t, true, isDense)

	isSparse, err := fI.GetSparse(0)
	assert.Nil(t, err)
	assert.Equal(t, false, isSparse)

	t1, t2, err := fI.GetTimestampRange(0)
	assert.Nil(t, err)
	assert.Equal(t, t2, t1)

	nonEmptyDomain, err := fI.GetNonEmptyDomainFromIndex(0, 0)
	assert.Nil(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	nonEmptyDomain, err = fI.GetNonEmptyDomainFromName(0, "dim1")
	assert.Nil(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	cellNum, err := fI.GetCellNum(0)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), cellNum)

	version, err := fI.GetVersion(0)
	assert.Nil(t, err)
	assert.Equal(t, uint32(7), version)

	hasConsolidatedMetadata, err := fI.HasConsolidatedMetadata(0)
	assert.Nil(t, err)
	assert.Equal(t, false, hasConsolidatedMetadata)

	unconsolidatedMetadataNum, err := fI.GetUnconsolidatedMetadataNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), unconsolidatedMetadataNum)

	toVacuumNum, err := fI.GetToVacuumNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), toVacuumNum)

	_, err = fI.GetToVacuumURI(0)
	assert.NotNil(t, err)

	fI.Free()
}
