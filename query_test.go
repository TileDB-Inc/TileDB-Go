package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ExampleNewQuery shows a complete write and read example
func ExampleNewQuery() {
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

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", []int8{0, 9}, int8(10))
	if err != nil {
		// Handle error
		return
	}

	// Test creating domain
	domain, err := NewDomain(context)
	if err != nil {
		// Handle error
		return
	}

	// Add dimension
	err = domain.AddDimensions(dimension)
	if err != nil {
		// Handle error
		return
	}

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	if err != nil {
		// Handle error
		return
	}

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	if err != nil {
		// Handle error
		return
	}

	// Crete attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	if err != nil {
		// Handle error
		return
	}

	// Crete attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	if err != nil {
		// Handle error
		return
	}

	// Crete attribute to add to schema
	attribute4, err := NewAttribute(context, "a4", TILEDB_STRING_UTF8)
	if err != nil {
		// Handle error
		return
	}

	// Set a3 to be variable length
	err = attribute3.SetCellValNum(TILEDB_VAR_NUM)
	if err != nil {
		// Handle error
		return
	}

	// Set a4 to be variable length
	err = attribute4.SetCellValNum(TILEDB_VAR_NUM)
	if err != nil {
		// Handle error
		return
	}

	// Add Attribute
	err = arraySchema.AddAttributes(attribute, attribute2, attribute3, attribute4)
	if err != nil {
		// Handle error
		return
	}

	// Set Domain
	err = arraySchema.SetDomain(domain)
	if err != nil {
		// Handle error
		return
	}

	// Validate Schema
	err = arraySchema.Check()
	if err != nil {
		// Handle error
		return
	}

	// create temp array name and path
	// normal usage would be "my_array" uri
	// Temp path is used here so unit test can clean up after itself
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_array"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	if err != nil {
		// Handle error
		return
	}

	// Create array on disk
	err = array.Create(arraySchema)
	if err != nil {
		// Handle error
		return
	}

	// Open array for writting
	err = array.Open(TILEDB_WRITE)
	if err != nil {
		// Handle error
		return
	}

	// Create write query
	query, err := NewQuery(context, array)
	if err != nil {
		// Handle error
		return
	}

	// Limit writting to subarray
	err = query.SetSubArray([]int8{0, 1})
	if err != nil {
		// Handle error
		return
	}

	// Set write layout
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	if err != nil {
		// Handle error
		return
	}

	// Create write buffers
	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	if err != nil {
		// Handle error
		return
	}

	bufferA2 := []byte("ab")
	_, err = query.SetBuffer("a2", bufferA2)
	if err != nil {
		// Handle error
		return
	}

	bufferA3 := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offsetBufferA3 := []uint64{0, 3}
	_, _, err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	if err != nil {
		// Handle error
		return
	}

	bufferA4 := []byte("hello" + "world")
	offsetBufferA4 := []uint64{0, 5}
	_, _, err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	if err != nil {
		// Handle error
		return
	}

	err = query.Submit()
	if err != nil {
		// Handle error
		return
	}

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	if err != nil {
		// Handle error
		return
	}
	if status != TILEDB_COMPLETED {
		// handle non-complete query
		// If applicable read partial data in buffer
		// and re-submit for remaining results
	}

	// Finalize Write
	err = query.Finalize()
	if err != nil {
		// Handle error
		return
	}

	// Close and prepare to read
	err = array.Close()
	if err != nil {
		// Handle error
		return
	}

	// Reopen array for reading
	err = array.Open(TILEDB_READ)
	if err != nil {
		// Handle error
		return
	}

	// Create query for reading
	query, err = NewQuery(context, array)
	if err != nil {
		// Handle error
		return
	}

	// Set read subarray to only data that was written
	err = query.SetSubArray([]int8{0, 1})
	if err != nil {
		// Handle error
		return
	}

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 2)
	_, err = query.SetBuffer("a1", readBufferA1)
	if err != nil {
		// Handle error
		return
	}

	readBufferA2 := make([]byte, 2)
	_, err = query.SetBuffer("a2", readBufferA2)
	if err != nil {
		// Handle error
		return
	}

	readBufferA3 := make([]float32, 5)
	readOffsetBufferA3 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a3", readOffsetBufferA3, readBufferA3)
	if err != nil {
		// Handle error
		return
	}
	readBufferA4 := make([]byte, 10)
	readOffsetBufferA4 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a4", readOffsetBufferA4, readBufferA4)
	if err != nil {
		// Handle error
		return
	}
	// Set read layout
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	if err != nil {
		// Handle error
		return
	}
	// Submit read query async
	// Async submits do not block
	err = query.SubmitAsync()
	if err != nil {
		// Handle error
		return
	}
	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		// Do something while query is running
	}
	if err != nil {
		// Handle error
		return
	}

	// Results should be returned
	hasResults, err := query.HasResults()
	if err != nil {
		// Handle error
		return
	}
	if hasResults {
		// Do something with read buffer
	}

}

func TestQueryEffectiveBufferSize(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", []int32{1, 4}, int32(2))
	assert.Nil(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", []int32{1, 4}, int32(2))
	assert.Nil(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	err = domain.AddDimensions(rowDim, colDim)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	err = arraySchema.SetAllowsDups(true)
	assert.Nil(t, err)

	allowDups, err := arraySchema.GetAllowsDups()
	assert.Nil(t, err)
	assert.Equal(t, true, allowDups)

	err = arraySchema.SetAllowsDups(false)
	assert.Nil(t, err)

	// Dense array, allowDups should be false
	allowDups, err = arraySchema.GetAllowsDups()
	assert.Nil(t, err)
	assert.Equal(t, false, allowDups)

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	err = attribute.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) +
		"tiledb_effective_buffer_size_array"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	err = query.SetLayout(TILEDB_GLOBAL_ORDER)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	assert.Nil(t, err)
	_, err = query.SetBuffer("rows", buffD1)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", buffD2)
	assert.Nil(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	assert.Nil(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	rowsDataSize, err := query.BufferSize("rows")
	assert.Nil(t, err)
	assert.Equal(t, len(buffD1), int(rowsDataSize))
	colsDataSize, err := query.BufferSize("cols")
	assert.Nil(t, err)
	assert.Equal(t, len(buffD2), int(colsDataSize))

	// Perform the write, finalize and close the array.
	err = query.Submit()
	assert.Nil(t, err)
	err = query.Finalize()
	assert.Nil(t, err)
	err = array.Close()
	assert.Nil(t, err)

	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Read value at cell 2, 2
	subArray := []int32{2, 2, 2, 2}

	// Prepare buffers
	rows := make([]int32, 2)
	cols := make([]int32, 2)
	// Allocate 4 bytes to store the read result
	a1DataRead := make([]byte, 4)
	a1OffRead := make([]uint64, 1)

	// Prepare the query
	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	err = query.SetSubArray(subArray)
	assert.Nil(t, err)
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rows)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", cols)
	assert.Nil(t, err)

	// Submit the query
	err = query.Submit()
	assert.Nil(t, err)

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	assert.Nil(t, err)
	assert.EqualValues(t, elements["a1"], [2]uint64{1, 3})
	assert.EqualValues(t, elements["rows"], [2]uint64{0, 1})
	assert.EqualValues(t, elements["cols"], [2]uint64{0, 1})

	query.Free()
}

func TestQueryEffectiveBufferSizeHeterogeneous(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", []int32{1, 4}, int32(2))
	assert.Nil(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", []int64{1, 4}, int64(2))
	assert.Nil(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	err = domain.AddDimensions(rowDim, colDim)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	err = attribute.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) +
		"tiledb_effective_buffer_size_array_heterogeneous"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []int32{1, 2, 2}
	colsWrite := []int64{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	err = query.SetLayout(TILEDB_GLOBAL_ORDER)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	assert.Nil(t, err)
	_, err = query.SetBuffer("rows", rowsWrite)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", colsWrite)
	assert.Nil(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	assert.Nil(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	dataSize, err = query.BufferSize("rows")
	assert.Nil(t, err)
	assert.Equal(t, len(rowsWrite), int(dataSize))
	dataSize, err = query.BufferSize("cols")
	assert.Nil(t, err)
	assert.Equal(t, len(colsWrite), int(dataSize))

	// Perform the write, finalize and close the array.
	err = query.Submit()
	assert.Nil(t, err)
	err = query.Finalize()
	assert.Nil(t, err)
	err = array.Close()
	assert.Nil(t, err)

	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Read value at cell 2, 2
	rowsRange := []int32{2, 2}
	colsRange := []int64{2, 2}

	// Prepare buffers
	rowsRead := make([]int32, 2)
	colsRead := make([]int64, 2)
	// Allocate 4 bytes to store the read result
	a1DataRead := make([]byte, 4)
	a1OffRead := make([]uint64, 1)

	// Prepare the query
	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	err = query.AddRange(0, rowsRange[0], rowsRange[1])
	assert.Nil(t, err)
	err = query.AddRange(1, colsRange[0], colsRange[1])
	assert.Nil(t, err)
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rowsRead)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", colsRead)
	assert.Nil(t, err)

	// Get Range for rows
	rangeStart, rangeEnd, err := query.GetRange(0, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Get Range for cols
	rangeStart, rangeEnd, err = query.GetRange(1, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, colsRange[0], rangeStart)
	assert.EqualValues(t, colsRange[1], rangeEnd)

	// Submit the query
	err = query.Submit()
	assert.Nil(t, err)

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	assert.Nil(t, err)
	assert.EqualValues(t, elements["a1"], [2]uint64{1, 3})
	assert.EqualValues(t, elements["rows"], [2]uint64{0, 1})
	assert.EqualValues(t, elements["cols"], [2]uint64{0, 1})

	query.Free()
}

func TestQueryEffectiveBufferSizeStrings(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create row dimension
	rowDim, err := NewStringDimension(context, "rows")
	assert.Nil(t, err)
	assert.NotNil(t, rowDim)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	err = domain.AddDimensions(rowDim)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	err = attribute.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) +
		"tiledb_effective_buffer_size_array_strings"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []byte("abbc")
	rowsOffWrite := []uint64{0, 1, 3}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	err = query.SetLayout(TILEDB_GLOBAL_ORDER)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("rows", rowsOffWrite, rowsWrite)
	assert.Nil(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	assert.Nil(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	offsetSize, dataSize, err = query.BufferSizeVar("rows")
	assert.Nil(t, err)
	assert.Equal(t, len(rowsOffWrite), int(offsetSize))
	assert.Equal(t, len(rowsWrite), int(dataSize))

	// Perform the write, finalize and close the array.
	err = query.Submit()
	assert.Nil(t, err)
	err = query.Finalize()
	assert.Nil(t, err)
	err = array.Close()
	assert.Nil(t, err)

	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Read value at cell "bb"
	rowsRange := [][]byte{[]byte("bb"), []byte("bb")}

	// Prepare buffers
	rowsRead := make([]byte, 4)
	rowsOffRead := make([]uint64, 2)
	// Allocate 4 bytes to store the read result
	a1DataRead := make([]byte, 4)
	a1OffRead := make([]uint64, 1)

	// Prepare the query
	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	err = query.AddRangeVar(0, rowsRange[0], rowsRange[1])
	assert.Nil(t, err)
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	_, _, err = query.SetBufferVar("rows", rowsOffRead, rowsRead)
	assert.Nil(t, err)

	// Get Range
	rangeStart, rangeEnd, err := query.GetRange(0, 0)
	assert.Nil(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Submit the query
	err = query.Submit()
	assert.Nil(t, err)

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 2)

	elements, err := query.ResultBufferElements()
	assert.Nil(t, err)
	assert.EqualValues(t, [2]uint64{1, 2}, elements["a1"])
	assert.EqualValues(t, [2]uint64{1, 2}, elements["rows"])

	query.Free()
}

func TestQueryEffectiveBufferSizeStringsHeterogeneous(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create row dimension
	rowDim, err := NewStringDimension(context, "rows")
	assert.Nil(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", []int64{1, 4}, int64(2))
	assert.Nil(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	err = domain.AddDimensions(rowDim, colDim)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	err = attribute.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) +
		"tiledb_effective_buffer_size_array_strings_heterogeneous"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []byte("abbc")
	rowsOffWrite := []uint64{0, 1, 3}
	colsWrite := []int64{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	err = query.SetLayout(TILEDB_GLOBAL_ORDER)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	assert.Nil(t, err)
	_, _, err = query.SetBufferVar("rows", rowsOffWrite, rowsWrite)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", colsWrite)
	assert.Nil(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	assert.Nil(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	offsetSize, dataSize, err = query.BufferSizeVar("rows")
	assert.Nil(t, err)
	assert.Equal(t, len(rowsOffWrite), int(offsetSize))
	assert.Equal(t, len(rowsWrite), int(dataSize))
	dataSize, err = query.BufferSize("cols")
	assert.Nil(t, err)
	assert.Equal(t, len(colsWrite), int(dataSize))

	// Perform the write, finalize and close the array.
	err = query.Submit()
	assert.Nil(t, err)
	err = query.Finalize()
	assert.Nil(t, err)
	err = array.Close()
	assert.Nil(t, err)

	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Read value at cell "c", 2
	rowsRange := [][]byte{[]byte("c"), []byte("c")}
	colsRange := []int64{2, 2}

	// Prepare buffers
	rowsRead := make([]byte, 4)
	rowsOffRead := make([]uint64, 2)
	colsRead := make([]int64, 2)
	// Allocate 4 bytes to store the read result
	a1DataRead := make([]byte, 4)
	a1OffRead := make([]uint64, 1)

	// Prepare the query
	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	err = query.AddRangeVar(0, rowsRange[0], rowsRange[1])
	assert.Nil(t, err)
	err = query.AddRange(1, colsRange[0], colsRange[1])
	assert.Nil(t, err)
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	assert.Nil(t, err)
	assert.NotNil(t, query)
	_, _, err = query.SetBufferVar("rows", rowsOffRead, rowsRead)
	assert.Nil(t, err)
	_, err = query.SetBuffer("cols", colsRead)
	assert.Nil(t, err)

	// Submit the query
	err = query.Submit()
	assert.Nil(t, err)

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	assert.Nil(t, err)
	assert.EqualValues(t, [2]uint64{1, 3}, elements["a1"])
	assert.EqualValues(t, [2]uint64{1, 1}, elements["rows"])
	assert.EqualValues(t, [2]uint64{0, 1}, elements["cols"])

	query.Free()
}

// TestQueryReadEmpty validates an empty array can be read from without error
func TestQueryReadEmpty(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", []int8{1, 10}, int8(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Create attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute2)

	// Create attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute3)

	// Crete attribute to add to schema
	attribute4, err := NewAttribute(context, "a4", TILEDB_STRING_UTF8)
	assert.Nil(t, err)
	assert.NotNil(t, attribute4)

	// Set a3 to be variable length
	err = attribute3.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Set a4 to be variable length
	err = attribute4.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute, attribute2, attribute3, attribute4)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_array"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	// Open array for reading
	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Create Query
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	// Limit reading to subArray
	err = query.SetSubArray([]int8{2, 4})
	assert.Nil(t, err)

	// Set buffer to incorrect type, should err
	bufferA1Bad := make([]int8, 2)
	_, err = query.SetBuffer("a1", bufferA1Bad)
	assert.NotNil(t, err)

	// Create read buffers
	bufferA1 := make([]int32, 2)
	_, err = query.SetBuffer("a1", bufferA1)
	assert.Nil(t, err)

	bufferA2 := make([]byte, 2)
	_, err = query.SetBuffer("a2", bufferA2)
	assert.Nil(t, err)

	bufferA3 := make([]float32, 5)
	offsetBufferA3 := make([]uint64, 3)
	_, _, err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	assert.Nil(t, err)

	bufferA4 := make([]byte, 4)
	offsetBufferA4 := make([]uint64, 4)
	_, _, err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	assert.Nil(t, err)

	// Set read layout
	assert.Nil(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit query
	assert.Nil(t, query.Submit())

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// No results because query it empty
	hasResults, err := query.HasResults()
	assert.Nil(t, err)
	assert.Equal(t, false, hasResults)

}

// TestQueryWrite validates a array can be written to and read from
func TestQueryWrite(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", []int8{0, 9}, int8(10))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Crete attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	assert.Nil(t, err)
	assert.NotNil(t, attribute2)

	// Crete attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute3)

	// Crete attribute to add to schema
	attribute4, err := NewAttribute(context, "a4", TILEDB_STRING_UTF8)
	assert.Nil(t, err)
	assert.NotNil(t, attribute4)

	// Crete attribute to add to schema
	attribute5, err := NewAttribute(context, "a5", TILEDB_CHAR)
	assert.Nil(t, err)
	assert.NotNil(t, attribute5)

	// Set a3 to be variable length
	err = attribute3.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Set a4 to be variable length
	err = attribute4.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Set a5 to be variable length
	err = attribute5.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute, attribute2, attribute3, attribute4, attribute5)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_array"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	// Open array for writting
	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)

	// Create write query
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	// Limit writting to subarray
	err = query.SetSubArray([]int8{0, 1})
	assert.Nil(t, err)

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Create write buffers
	bufferDim1 := []int8{4, 6}
	_, err = query.SetBuffer("dim1", bufferDim1)
	assert.Nil(t, err)

	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	assert.Nil(t, err)

	bufferA2 := []byte("ab")
	_, err = query.SetBuffer("a2", bufferA2)
	assert.Nil(t, err)

	bufferA3 := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offsetBufferA3 := []uint64{0, 3}
	_, _, err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	assert.Nil(t, err)

	bufferA4 := []byte("hello" + "world")
	offsetBufferA4 := []uint64{0, 5}
	// Second byte array so we can compare reads
	bufferA4Comparison := make([]byte, len(bufferA4))
	elementsCopied := copy(bufferA4Comparison, bufferA4)
	assert.Equal(t, len(bufferA4), elementsCopied)

	_, _, err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	assert.Nil(t, err)

	bufferA5 := "hello" + "world"
	offsetBufferA5 := []uint64{0, 5}
	// Second string array so we can compare reads
	bufferA5Comparison := make([]byte, len(bufferA5)) //new(string, len(bufferA5))
	elementsCopied = copy(bufferA5Comparison, bufferA5)
	assert.Equal(t, len(bufferA5), elementsCopied)
	assert.EqualValues(t, bufferA5, bufferA5Comparison)
	bufferA5Bytes := []byte(bufferA5)

	_, _, err = query.SetBufferVar("a5", offsetBufferA5, bufferA5Bytes)
	// Immediately set bufferA5 to nil to validate underlying array is not GC'ed
	//bufferA5 = nil
	assert.Nil(t, err)

	// Submit write query
	err = query.Submit()
	assert.Nil(t, err)

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	err = query.Finalize()
	assert.Nil(t, err)
	// Close and prepare to read
	err = array.Close()
	assert.Nil(t, err)

	// Reopen array for reading
	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Get non empty domain, which should not be empty
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	assert.Nil(t, err)
	assert.NotNil(t, nonEmptyDomain)
	assert.False(t, isEmpty)
	assert.EqualValues(t, []NonEmptyDomain{{DimensionName: "dim1", Bounds: []int8{0, 1}}}, nonEmptyDomain)

	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	// Set read subarray to only data that was written
	subArray := []int8{0, 1}
	err = query.SetSubArray(subArray)
	assert.Nil(t, err)

	bufferElements, err := query.EstimateBufferElements()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), bufferElements["a1"][0])
	assert.Equal(t, uint64(2), bufferElements["a1"][1])
	assert.Equal(t, uint64(0), bufferElements["a2"][0])
	assert.Equal(t, uint64(2), bufferElements["a2"][1])
	assert.Equal(t, uint64(2), bufferElements["a3"][0])
	assert.Equal(t, uint64(2), bufferElements["a3"][1])
	assert.Equal(t, uint64(2), bufferElements["a4"][0])
	assert.Equal(t, uint64(4), bufferElements["a4"][1])
	assert.Equal(t, uint64(2), bufferElements["a5"][0])
	assert.Equal(t, uint64(4), bufferElements["a5"][1])

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 2)
	_, err = query.SetBuffer("a1", readBufferA1)
	assert.Nil(t, err)

	readBufferA2 := make([]byte, 2)
	_, err = query.SetBuffer("a2", readBufferA2)
	assert.Nil(t, err)

	readBufferA3 := make([]float32, 5)
	readOffsetBufferA3 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a3", readOffsetBufferA3, readBufferA3)
	assert.Nil(t, err)

	readBufferA4 := make([]byte, 10)
	readOffsetBufferA4 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a4", readOffsetBufferA4, readBufferA4)
	assert.Nil(t, err)

	readBufferA5 := make([]byte, 10) //make(string, 10)
	readOffsetBufferA5 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a5", readOffsetBufferA5, readBufferA5)
	assert.Nil(t, err)

	// Set read layout
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Submit read query async
	err = query.SubmitAsync()
	assert.Nil(t, err)

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		assert.Nil(t, err)
		assert.Equal(t, TILEDB_INPROGRESS, status)
	}
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	assert.Nil(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	assert.EqualValues(t, bufferA1, readBufferA1)
	assert.EqualValues(t, bufferA2, readBufferA2)
	assert.EqualValues(t, bufferA3, readBufferA3)
	assert.EqualValues(t, bufferA4Comparison, readBufferA4)
	assert.EqualValues(t, bufferA5Comparison, readBufferA5)

	bufferA1InterfaceGet, err := query.Buffer("a1")
	assert.Nil(t, err)
	assert.EqualValues(t, bufferA1, bufferA1InterfaceGet.([]int32))

	offsetsBufferA4Get, bufferA4InterfaceGet, err := query.BufferVar("a4")
	assert.Nil(t, err)
	assert.EqualValues(t, bufferA4Comparison, bufferA4InterfaceGet.([]byte))
	assert.EqualValues(t, offsetBufferA4, offsetsBufferA4Get)

	offsetsBufferA5Get, bufferA5InterfaceGet, err := query.BufferVar("a5")
	assert.Nil(t, err)
	assert.EqualValues(t, bufferA5Comparison, bufferA5InterfaceGet.([]byte))
	assert.EqualValues(t, offsetBufferA5, offsetsBufferA5Get)

	query.Free()
}

// TestSparseQueryWrite validates a sparse array can be written to and read from
func TestSparseQueryWrite(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension

	dimension, err := NewDimension(context, "dim1", []int8{0, 9}, int8(10))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
	assert.Nil(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Validate Schema
	err = arraySchema.Check()
	assert.Nil(t, err)

	// create temp group name
	tmpArrayPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_sparse_array"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	// Open array for writting
	err = array.Open(TILEDB_WRITE)
	assert.Nil(t, err)

	// Create write query
	query, err := NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_UNORDERED))

	// Create write buffers
	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	assert.Nil(t, err)

	// Set coordinates, since test is 1d, this is subarray
	subArray := []int8{0, 1}
	_, err = query.SetBuffer("dim1", subArray)
	assert.Nil(t, err)

	// Submit write query
	err = query.Submit()
	assert.Nil(t, err)

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	err = query.Finalize()
	assert.Nil(t, err)
	// Close and prepare to read
	err = array.Close()
	assert.Nil(t, err)

	// Reopen array for reading
	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	query, err = NewQuery(context, array)
	assert.Nil(t, err)
	assert.NotNil(t, query)

	// Set read subarray to only data that was written
	//err = query.SetSubArray(subArray)
	//assert.Nil(t, err)

	// Set coordinates, since test is 1d, this is subarray
	_, err = query.SetBuffer("dim1", subArray)
	assert.Nil(t, err)

	bufferElements, err := query.EstimateBufferElements()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), bufferElements["a1"][0])
	assert.Equal(t, uint64(2), bufferElements["a1"][1])

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 2)
	_, err = query.SetBuffer("a1", readBufferA1)
	assert.Nil(t, err)

	// Set read layout
	err = query.SetLayout(TILEDB_ROW_MAJOR)
	assert.Nil(t, err)

	// Submit read query async
	err = query.SubmitAsync()
	assert.Nil(t, err)

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		assert.Nil(t, err)
		assert.Equal(t, TILEDB_INPROGRESS, status)
	}
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	assert.Nil(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	assert.EqualValues(t, bufferA1, readBufferA1)
}
