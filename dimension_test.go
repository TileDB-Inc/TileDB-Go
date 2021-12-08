package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleNewDimension() {
	// Create Config, this is optional
	config, err := NewConfig()
	if err != nil {
		// Handle error
		return
	}

	// Test context with config
	context, err := NewContext(config)
	if err != nil {
		// Handle error
		return
	}

	// Create Dimension
	dim, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	if err != nil {
		// Handle error
		return
	}

	// Set Filter List
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	if err != nil {
		// Handle error
		return
	}

	filterList, err := NewFilterList(context)
	if err != nil {
		// Handle error
		return
	}

	err = filterList.AddFilter(filter)
	if err != nil {
		// Handle error
		return
	}

	err = dim.SetFilterList(filterList)
	if err != nil {
		// Handle error
		return
	}
}

// TestDimension tests creating a new dimension
func TestDimension(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Dimension will error due to extent and domain having different datatypes
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, 5)
	assert.Error(t, err)
	assert.Nil(t, dimension)

	// Create dimension
	dimension, err = NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	name, err := dimension.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", name)

	datatype, err := dimension.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, dimension.SetFilterList(filterList))

	filterListReturn, err := dimension.FilterList()
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err := filterListReturn.FilterFromIndex(0)
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err := filterReturn.Type()
	require.NoError(t, err)
	assert.EqualValues(t, TILEDB_FILTER_GZIP, filterTypeReturn)
	filterOption, err := filter.Option(TILEDB_COMPRESSION_LEVEL)
	require.NoError(t, err)
	assert.EqualValues(t, int32(5), filterOption)

	dimension.Free()
}

// TestDimensionDomainTypes tests creating dimension of all domain types
func TestDimensionDomainTypes(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	dimension, err := NewDimension(context, "test", TILEDB_INT64, []int64{1, 10}, int64(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	domain, err := dimension.Domain()
	require.NoError(t, err)
	// Test getting domain
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int8{1, 10}, domain)
	extent, err := dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int8(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_INT16, []int16{1, 10}, int16(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int16{1, 10}, domain)
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int16(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int32{1, 10}, domain)
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int32(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_INT64, []int64{1, 10}, int64(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int64{1, 10}, domain)
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int64(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_UINT64, []uint64{1, 10}, uint64(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", TILEDB_UINT8, []uint8{1, 10}, uint8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint8{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint8(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_UINT16, []uint16{1, 10}, uint16(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint16{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint16(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_UINT32, []uint32{1, 10}, uint32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint32(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_UINT64, []uint64{1, 10}, uint64(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint64(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_FLOAT32, []float32{1, 10}, float32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float32(5), extent)

	dimension, err = NewDimension(context, "test", TILEDB_FLOAT64, []float64{1, 10}, float64(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float64(5), extent)

	dimension, err = NewStringDimension(context, "test")
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	domain, err = dimension.Domain()
	require.NoError(t, err)
	// Test getting domain
	assert.Nil(t, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	require.NoError(t, err)
	assert.Nil(t, extent)

	// Temp path for testing dump
	tmpPathDump := filepath.Join(t.TempDir(), "dump")

	// Test dumping to file
	require.NoError(t, dimension.Dump(tmpPathDump))
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	require.NoError(t, dimension.DumpSTDOUT())
}
