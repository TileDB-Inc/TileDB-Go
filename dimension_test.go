package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	dim, err := NewDimension(context, "test", []int32{1, 10}, int32(5))
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
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Dimension will error due to extent and domain having different datatypes
	dimension, err := NewDimension(context, "test", []int32{1, 10}, 5)
	assert.NotNil(t, err)
	assert.Nil(t, dimension)

	// Create dimension
	dimension, err = NewDimension(context, "test", []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	name, err := dimension.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", name)

	datatype, err := dimension.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	assert.Nil(t, err)
	err = filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5))
	assert.Nil(t, err)
	filterList, err := NewFilterList(context)
	assert.Nil(t, err)
	err = filterList.AddFilter(filter)
	assert.Nil(t, err)
	err = dimension.SetFilterList(filterList)
	assert.Nil(t, err)

	filterListReturn, err := dimension.FilterList()
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err := filterListReturn.FilterFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err := filterReturn.Type()
	assert.Nil(t, err)
	assert.EqualValues(t, TILEDB_FILTER_GZIP, filterTypeReturn)
	filterOption, err := filter.Option(TILEDB_COMPRESSION_LEVEL)
	assert.Nil(t, err)
	assert.EqualValues(t, int32(5), filterOption)

	dimension.Free()
}

// TestDimensionDomainTypes tests creating dimension of all domain types
func TestDimensionDomainTypes(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	dimension, err := NewDimension(context, "test", []int{1, 10}, int(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", []int8{1, 10}, int8(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	domain, err := dimension.Domain()
	assert.Nil(t, err)
	// Test getting domain
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int8{1, 10}, domain)
	extent, err := dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int8(5), extent)

	dimension, err = NewDimension(context, "test", []int16{1, 10}, int16(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int16{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int16(5), extent)

	dimension, err = NewDimension(context, "test", []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int32{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int32(5), extent)

	dimension, err = NewDimension(context, "test", []int64{1, 10}, int64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int64{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int64(5), extent)

	dimension, err = NewDimension(context, "test", []uint{1, 10}, uint(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", []uint8{1, 10}, uint8(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint8{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint8(5), extent)

	dimension, err = NewDimension(context, "test", []uint16{1, 10}, uint16(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint16{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint16(5), extent)

	dimension, err = NewDimension(context, "test", []uint32{1, 10}, uint32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint32(5), extent)

	dimension, err = NewDimension(context, "test", []uint64{1, 10}, uint64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint64(5), extent)

	dimension, err = NewDimension(context, "test", []float32{1, 10}, float32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float32(5), extent)

	dimension, err = NewDimension(context, "test", []float64{1, 10}, float64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float64(5), extent)

	dimension, err = NewStringDimension(context, "test")
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	// Test getting domain
	assert.Nil(t, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.Nil(t, extent)

	// Temp path for testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_dimension_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	err = dimension.Dump(tmpPathDump)
	assert.Nil(t, err)
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	err = dimension.DumpSTDOUT()
	assert.Nil(t, err)
}
