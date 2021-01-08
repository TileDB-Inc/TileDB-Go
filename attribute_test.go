package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	assert.Nil(t, err)
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
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Get Attribute Name
	name, err := attribute.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", name)

	// Get Attribute Datatype
	datatype, err := attribute.Type()
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
	err = attribute.SetFilterList(filterList)
	assert.Nil(t, err)

	filterListReturn, err := attribute.FilterList()
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

	// Set Cell Value Number
	err = attribute.SetCellValNum(10)
	assert.Nil(t, err)

	// Get attribute cell size
	cellSize, err := attribute.CellSize()
	assert.Nil(t, err)
	assert.EqualValues(t, 40, cellSize)

	cellValNum, err := attribute.CellValNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(10), cellValNum)

	err = attribute.SetFillValue(12)
	assert.Nil(t, err)

	fillValue, valueSize, err := attribute.GetFillValue()
	assert.Nil(t, err)
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
	err = attribute.Dump(tmpPathDump)
	assert.Nil(t, err)
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	err = attribute.DumpSTDOUT()
	assert.Nil(t, err)
}

func TestNullableAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Get Attribute Name
	name, err := attribute.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", name)

	// Set Attribute Nullable
	err = attribute.SetNullable(true)
	assert.Nil(t, err)

	// Get Attribute Nullable
	nullable, err := attribute.Nullable()
	assert.Nil(t, err)
	assert.True(t, nullable)

	// Get Attribute Datatype
	datatype, err := attribute.Type()
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
	err = attribute.SetFilterList(filterList)
	assert.Nil(t, err)

	filterListReturn, err := attribute.FilterList()
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

	// Set Cell Value Number
	err = attribute.SetCellValNum(10)
	assert.Nil(t, err)

	// Get attribute cell size
	cellSize, err := attribute.CellSize()
	assert.Nil(t, err)
	assert.EqualValues(t, 40, cellSize)

	cellValNum, err := attribute.CellValNum()
	assert.Nil(t, err)
	assert.Equal(t, uint32(10), cellValNum)

	err = attribute.SetFillValueNullable(12, false)
	assert.Nil(t, err)

	fillValue, valueSize, nullable, err := attribute.GetFillValueNullable()
	assert.Nil(t, err)
	assert.Equal(t, int32(12), fillValue)
	assert.Equal(t, uint64(40), valueSize)
	assert.False(t, nullable)

	// Temp path for testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_attribute_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	err = attribute.Dump(tmpPathDump)
	assert.Nil(t, err)
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	err = attribute.DumpSTDOUT()
	assert.Nil(t, err)
}
