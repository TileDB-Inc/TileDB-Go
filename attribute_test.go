package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleNewAttribute() {
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

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
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
	err = attribute.SetFilterList(filterList)
	if err != nil {
		// Handle error
		return
	}

	// Set Cell Value Number
	err = attribute.SetCellValNum(10)
	if err != nil {
		// Handle error
		return
	}
}

//TestNewAttribute tests setting a new context
func TestNewAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	attribute.Free()
}

func ExampleAttribute_SetFilterList() {
	// Create configuration
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

	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
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

	err = filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5))
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
	err = attribute.SetFilterList(filterList)
	if err != nil {
		// Handle error
		return
	}
}

func TestFullAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Get Attribute Name
	name, err := attribute.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", name)

	// Get Attribute Datatype
	datatype, err := attribute.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, attribute.SetFilterList(filterList))

	filterListReturn, err := attribute.FilterList()
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

	// Set Cell Value Number
	require.NoError(t, attribute.SetCellValNum(10))

	// Get attribute cell size
	cellSize, err := attribute.CellSize()
	require.NoError(t, err)
	assert.EqualValues(t, 40, cellSize)

	cellValNum, err := attribute.CellValNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(10), cellValNum)

	require.NoError(t, attribute.SetFillValue(12))

	fillValue, valueSize, err := attribute.GetFillValue()
	require.NoError(t, err)
	assert.Equal(t, int32(12), fillValue)
	assert.Equal(t, uint64(40), valueSize)

	// Temp path for testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_attribute_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	require.NoError(t, attribute.Dump(tmpPathDump))
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	require.NoError(t, attribute.DumpSTDOUT())
}

func TestNullableAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Get Attribute Name
	name, err := attribute.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", name)

	// Set Attribute Nullable
	require.NoError(t, attribute.SetNullable(true))

	// Get Attribute Nullable
	nullable, err := attribute.Nullable()
	require.NoError(t, err)
	assert.True(t, nullable)

	// Get Attribute Datatype
	datatype, err := attribute.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, attribute.SetFilterList(filterList))

	filterListReturn, err := attribute.FilterList()
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

	// Set Cell Value Number
	require.NoError(t, attribute.SetCellValNum(10))

	// Get attribute cell size
	cellSize, err := attribute.CellSize()
	require.NoError(t, err)
	assert.EqualValues(t, 40, cellSize)

	cellValNum, err := attribute.CellValNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(10), cellValNum)

	require.NoError(t, attribute.SetFillValueNullable(12, true))

	fillValue, valueSize, valid, err := attribute.GetFillValueNullable()
	require.NoError(t, err)
	assert.Equal(t, int32(12), fillValue)
	assert.Equal(t, uint64(40), valueSize)
	assert.True(t, valid)

	// Temp path for testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_attribute_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	require.NoError(t, attribute.Dump(tmpPathDump))
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	require.NoError(t, attribute.DumpSTDOUT())
}
