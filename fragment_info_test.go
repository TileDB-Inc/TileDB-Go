package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFragmentInfo(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	fragmentSize := testFragmentInfo(t, context)
	assert.Equal(t, uint64(4290), fragmentSize)
}

func TestFragmentInfoEncryption(t *testing.T) {
	encryption_key := "unittestunittestunittestunittest"
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	err = config.Set("sm.encryption_type", TILEDB_AES_256_GCM.String())
	require.NoError(t, err)

	err = config.Set("sm.encryption_key", encryption_key)
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	fragmentSize := testFragmentInfo(t, context)
	assert.Equal(t, uint64(7601), fragmentSize)
}

func testFragmentInfo(t testing.TB, context *Context) uint64 {
	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Get array URI
	uri, err := array.URI()
	require.NoError(t, err)
	assert.Equal(t, "file://"+tmpArrayPath, uri)

	// Close Array
	require.NoError(t, array.Close())

	array.Free()

	// Create new fragment info struct
	fI, err := NewFragmentInfo(context, uri)
	require.NoError(t, err)
	assert.NotNil(t, fI)

	// Load fragment info
	require.NoError(t, fI.Load())

	num, err := fI.GetFragmentNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), num)

	writeToArray(t, context, tmpArrayPath)

	// Load fragment info again
	require.NoError(t, fI.Load())

	num, err = fI.GetFragmentNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), num)

	fragmentURI, err := fI.GetFragmentURI(0)
	require.NoError(t, err)
	assert.NotEmpty(t, fragmentURI)

	fragmentSize, err := fI.GetFragmentSize(0)
	require.NoError(t, err)

	isDense, err := fI.GetDense(0)
	require.NoError(t, err)
	assert.Equal(t, true, isDense)

	isSparse, err := fI.GetSparse(0)
	require.NoError(t, err)
	assert.Equal(t, false, isSparse)

	t1, t2, err := fI.GetTimestampRange(0)
	require.NoError(t, err)
	assert.Equal(t, t2, t1)

	nonEmptyDomain, err := fI.GetNonEmptyDomainFromIndex(0, 0)
	require.NoError(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	nonEmptyDomain, err = fI.GetNonEmptyDomainFromName(0, "dim1")
	require.NoError(t, err)
	assert.Equal(t, "dim1", nonEmptyDomain.DimensionName)
	assert.Equal(t, []int8{1, 10}, nonEmptyDomain.Bounds)

	cellNum, err := fI.GetCellNum(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), cellNum)

	version, err := fI.GetVersion(0)
	require.NoError(t, err)
	assert.Greater(t, version, uint32(0))

	hasConsolidatedMetadata, err := fI.HasConsolidatedMetadata(0)
	require.NoError(t, err)
	assert.Equal(t, false, hasConsolidatedMetadata)

	unconsolidatedMetadataNum, err := fI.GetUnconsolidatedMetadataNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), unconsolidatedMetadataNum)

	toVacuumNum, err := fI.GetToVacuumNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), toVacuumNum)

	_, err = fI.GetToVacuumURI(0)
	assert.Error(t, err)

	framentInfoStr, err := fI.String()
	require.NoError(t, err)
	assert.NotEmpty(t, framentInfoStr)

	fI.Free()

	return fragmentSize
}

func writeToArray(t testing.TB, context *Context, tmpArrayPath string) {
	// Prepare some data for the array
	a1 := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	a2 := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}
	a2Off := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}

	// Create the query
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	_, err = query.SetDataBuffer("a1", a1)
	require.NoError(t, err)
	_, err = query.SetDataBuffer("a2", a2)
	require.NoError(t, err)
	_, err = query.SetOffsetsBuffer("a2", a2Off)
	require.NoError(t, err)

	require.NoError(t, query.Submit())
	require.NoError(t, array.Close())
}
