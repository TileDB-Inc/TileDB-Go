package tiledb

import (
	"os"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
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

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	if err != nil {
		// Handle error
		return
	}

	// Create attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	if err != nil {
		// Handle error
		return
	}

	// Create attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	if err != nil {
		// Handle error
		return
	}

	// Create attribute to add to schema
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
	tmpArrayPath, err := os.MkdirTemp("", "tiledb_test_array")
	if err != nil {
		// Handle error
		return
	}
	defer os.RemoveAll(tmpArrayPath)
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

	stats, err := query.Stats()
	if err != nil {
		// Handle error
		return
	}

	if len(stats) > 0 {
		// Do something with stats
	}
}

// ExampleNewQuery shows a complete write, delete and read example
func ExampleDeleteQuery() {
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
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
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
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	if err != nil {
		// Handle error
		return
	}

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	if err != nil {
		// Handle error
		return
	}

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
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

	err = arraySchema.SetCellOrder(TILEDB_ROW_MAJOR)
	if err != nil {
		// Handle error
		return
	}

	err = arraySchema.SetTileOrder(TILEDB_ROW_MAJOR)
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

	// create temp group name
	tmpArrayPath := os.TempDir()
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

	// Set write layout
	err = query.SetLayout(TILEDB_UNORDERED)
	if err != nil {
		// Handle error
		return
	}

	// Create write buffers
	bufferA1 := []int32{1, 2, 3, 4}
	_, err = query.SetBuffer("a1", bufferA1)
	if err != nil {
		// Handle error
		return
	}

	// Set coordinates, since test is 1d, this is subarray
	subArray := []int8{0, 1, 2, 3}
	_, err = query.SetBuffer("dim1", subArray)
	if err != nil {
		// Handle error
		return
	}

	// Submit write query
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

	// Validate query type
	_, err = query.Type()
	if err != nil {
		// Handle error
		return
	}

	// Finalize Write
	err = query.Finalize()
	if err != nil {
		// Handle error
		return
	}
	// Close and prepare to delete
	err = array.Close()
	if err != nil {
		// Handle error
		return
	}

	// Prepare a Delete query for elements dim1[2,3]
	// Reopen array for deletion
	err = array.Open(TILEDB_DELETE)
	if err != nil {
		// Handle error
		return
	}

	query, err = NewQuery(context, array)
	if err != nil {
		// Handle error
		return
	}

	condition, err := NewQueryCondition(context, "dim1", TILEDB_QUERY_CONDITION_GE, int8(2))
	if err != nil {
		// Handle error
		return
	}

	err = query.SetQueryCondition(condition)
	if err != nil {
		// Handle error
		return
	}

	// submit and finalize query
	err = query.Submit()
	if err != nil {
		// Handle error
		return
	}
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

	query, err = NewQuery(context, array)
	if err != nil {
		// Handle error
		return
	}

	// Set coordinates, since test is 1d, this is subarray
	_, err = query.SetBuffer("dim1", subArray)
	if err != nil {
		// Handle error
		return
	}

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 4)
	_, err = query.SetBuffer("a1", readBufferA1)
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
	err = query.SubmitAsync()
	if err != nil {
		// Handle error
		return
	}

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		if err != nil {
			// Handle error
			return
		}
	}
	if err != nil {
		// Handle error
		return
	}

	// Validate query type
	_, err = query.Type()
	if err != nil {
		// Handle error
		return
	}

	// Results should be returned
	_, err = query.HasResults()
	if err != nil {
		// Handle error
		return
	}
}

func TestQueryEffectiveBufferSize(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim, colDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetAllowsDups(true))

	allowDups, err := arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, true, allowDups)

	require.NoError(t, arraySchema.SetAllowsDups(false))

	// Dense array, allowDups should be false
	allowDups, err = arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, false, allowDups)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("rows", buffD1)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", buffD2)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	rowsDataSize, err := query.BufferSize("rows")
	require.NoError(t, err)
	assert.Equal(t, len(buffD1), int(rowsDataSize))
	colsDataSize, err := query.BufferSize("cols")
	require.NoError(t, err)
	assert.Equal(t, len(buffD2), int(colsDataSize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

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
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.SetSubArray(subArray))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rows)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", cols)
	require.NoError(t, err)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 3, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["rows"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["cols"])

	query.Free()
}

func TestQueryEffectiveBufferSizeHeterogeneous(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", TILEDB_INT64, []int64{1, 4}, int64(2))
	require.NoError(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim, colDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute2)

	require.NoError(t, attribute2.SetCellValNum(TILEDB_VAR_NUM))

	require.NoError(t, attribute2.SetNullable(true))

	attribute3, err := NewAttribute(context, "a3", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute3)

	require.NoError(t, attribute3.SetNullable(true))

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	require.NoError(t, arraySchema.AddAttributes(attribute2))

	require.NoError(t, arraySchema.AddAttributes(attribute3))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []int32{1, 2, 2}
	colsWrite := []int64{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}
	a2DataWrite := []byte("bccddd")
	a2OffWrite := []uint64{0, 1, 3}
	a2Validity := []uint8{1, 1, 0}
	a3DataWrite := []byte("abc")
	a3Validity := []uint8{1, 1, 0}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("rows", rowsWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", colsWrite)
	require.NoError(t, err)
	_, _, _, err = query.SetBufferVarNullable("a2", a2OffWrite, a2DataWrite, a2Validity)
	require.NoError(t, err)
	_, _, err = query.SetBufferNullable("a3", a3DataWrite, a3Validity)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	dataSize, err = query.BufferSize("rows")
	require.NoError(t, err)
	assert.Equal(t, len(rowsWrite), int(dataSize))
	dataSize, err = query.BufferSize("cols")
	require.NoError(t, err)
	assert.Equal(t, len(colsWrite), int(dataSize))
	offsetSize, dataSize, validitySize, err := query.BufferSizeVarNullable("a2")
	require.NoError(t, err)
	assert.Equal(t, len(a2OffWrite), int(offsetSize))
	assert.Equal(t, len(a2DataWrite), int(dataSize))
	assert.Equal(t, len(a2Validity), int(validitySize))
	dataSize, validitySize, err = query.BufferSizeNullable("a3")
	require.NoError(t, err)
	assert.Equal(t, len(a3DataWrite), int(dataSize))
	assert.Equal(t, len(a3Validity), int(validitySize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())

	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

	nonEmptyDomainMap, err := array.NonEmptyDomainMap()
	require.NoError(t, err)
	assert.EqualValues(t, 2, len(nonEmptyDomainMap))
	rowNonEmptyDomain := nonEmptyDomainMap["rows"].([]int32)
	colNonEmptyDomain := nonEmptyDomainMap["rows"].([]int32)
	assert.EqualValues(t, []int32{1, 2}, rowNonEmptyDomain)
	assert.EqualValues(t, []int32{1, 2}, colNonEmptyDomain)

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
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.AddRange(0, rowsRange[0], rowsRange[1]))
	require.NoError(t, query.AddRange(1, colsRange[0], colsRange[1]))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rowsRead)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", colsRead)
	require.NoError(t, err)

	// Get Range for rows
	rangeStart, rangeEnd, err := query.GetRange(0, 0)
	require.NoError(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Get Range for cols
	rangeStart, rangeEnd, err = query.GetRange(1, 0)
	require.NoError(t, err)
	assert.EqualValues(t, colsRange[0], rangeStart)
	assert.EqualValues(t, colsRange[1], rangeEnd)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 3, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["rows"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["cols"])

	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	// Reopen the array
	require.NoError(t, array.Open(TILEDB_READ))

	// Prepare the query for add / get ranges by name
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.AddRangeByName("rows", rowsRange[0], rowsRange[1]))
	require.NoError(t, query.AddRangeByName("cols", colsRange[0], colsRange[1]))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err = query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rowsRead)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", colsRead)
	require.NoError(t, err)

	// Get Range for rows
	rangeStart, rangeEnd, err = query.GetRangeFromName("rows", 0)
	require.NoError(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Get Range for cols
	rangeStart, rangeEnd, err = query.GetRangeFromName("cols", 0)
	require.NoError(t, err)
	assert.EqualValues(t, colsRange[0], rangeStart)
	assert.EqualValues(t, colsRange[1], rangeEnd)

	// Submit the query
	require.NoError(t, query.Submit())

	require.NoError(t, query.Finalize())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err = query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 3, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["rows"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["cols"])

	query.Free()
}

func TestQueryEffectiveBufferSizeStrings(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewStringDimension(context, "rows")
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []byte("abbc")
	rowsOffWrite := []uint64{0, 1, 3}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, _, err = query.SetBufferVar("rows", rowsOffWrite, rowsWrite)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	offsetSize, dataSize, err = query.BufferSizeVar("rows")
	require.NoError(t, err)
	assert.Equal(t, len(rowsOffWrite), int(offsetSize))
	assert.Equal(t, len(rowsWrite), int(dataSize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

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
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.AddRangeVar(0, rowsRange[0], rowsRange[1]))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, _, err = query.SetBufferVar("rows", rowsOffRead, rowsRead)
	require.NoError(t, err)

	// Get Range
	rangeStart, rangeEnd, err := query.GetRange(0, 0)
	require.NoError(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 2)

	elements, err := query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 2, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{1, 2, 0}, elements["rows"])

	require.NoError(t, query.Finalize())

	require.NoError(t, array.Close())

	// Re open the array
	require.NoError(t, array.Open(TILEDB_READ))

	nonEmptyDomainMap, err := array.NonEmptyDomainMap()
	require.NoError(t, err)
	assert.EqualValues(t, 1, len(nonEmptyDomainMap))
	rowNonEmptyDomain := nonEmptyDomainMap["rows"].([]string)
	assert.EqualValues(t, []string{"a", "c"}, rowNonEmptyDomain)

	// Prepare the query
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.AddRangeVarByName("rows", rowsRange[0], rowsRange[1]))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err = query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, _, err = query.SetBufferVar("rows", rowsOffRead, rowsRead)
	require.NoError(t, err)

	// Get Range
	rangeStart, rangeEnd, err = query.GetRangeFromName("rows", 0)
	require.NoError(t, err)
	assert.EqualValues(t, rowsRange[0], rangeStart)
	assert.EqualValues(t, rowsRange[1], rangeEnd)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 2)

	elements, err = query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 2, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{1, 2, 0}, elements["rows"])

	query.Free()
}

func TestQueryEffectiveBufferSizeStringsHeterogeneous(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewStringDimension(context, "rows")
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", TILEDB_INT64, []int64{1, 4}, int64(2))
	require.NoError(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim, colDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	rowsWrite := []byte("abbc")
	rowsOffWrite := []uint64{0, 1, 3}
	colsWrite := []int64{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, _, err = query.SetBufferVar("rows", rowsOffWrite, rowsWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", colsWrite)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	offsetSize, dataSize, err = query.BufferSizeVar("rows")
	require.NoError(t, err)
	assert.Equal(t, len(rowsOffWrite), int(offsetSize))
	assert.Equal(t, len(rowsWrite), int(dataSize))
	dataSize, err = query.BufferSize("cols")
	require.NoError(t, err)
	assert.Equal(t, len(colsWrite), int(dataSize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

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
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.AddRangeVar(0, rowsRange[0], rowsRange[1]))
	require.NoError(t, query.AddRange(1, colsRange[0], colsRange[1]))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, _, err = query.SetBufferVar("rows", rowsOffRead, rowsRead)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", colsRead)
	require.NoError(t, err)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 3, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{1, 1, 0}, elements["rows"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["cols"])

	query.Free()
}

// TestQueryReadEmpty validates an empty array can be read from without error
func TestQueryReadEmpty(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Create attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute2)

	// Create attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute3)

	// Create attribute to add to schema
	attribute4, err := NewAttribute(context, "a4", TILEDB_STRING_UTF8)
	require.NoError(t, err)
	assert.NotNil(t, attribute4)

	// Set a3 to be variable length
	require.NoError(t, attribute3.SetCellValNum(TILEDB_VAR_NUM))

	// Set a4 to be variable length
	require.NoError(t, attribute4.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute, attribute2, attribute3, attribute4))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Open array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	// Create Query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Limit reading to subArray
	require.NoError(t, query.SetSubArray([]int8{2, 4}))

	// Set buffer to incorrect type, should err
	bufferA1Bad := make([]int8, 4)
	_, err = query.SetBuffer("a1", bufferA1Bad)
	assert.Error(t, err)

	// Create read buffers
	bufferA1 := make([]int32, 4)
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)

	bufferA2 := make([]byte, 4)
	_, err = query.SetBuffer("a2", bufferA2)
	require.NoError(t, err)

	bufferA3 := make([]float32, 10)
	offsetBufferA3 := make([]uint64, 6)
	_, _, err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	require.NoError(t, err)

	bufferA4 := make([]byte, 8)
	offsetBufferA4 := make([]uint64, 8)
	_, _, err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	require.NoError(t, err)

	// Set read layout
	assert.Nil(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit query
	assert.Nil(t, query.Submit())

	// Validate status, since array was empty this should be completed
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Has results since dense arrays always return the fill-in values
	hasResults, err := query.HasResults()
	require.NoError(t, err)
	assert.Equal(t, true, hasResults)

}

// TestDenseQueryWrite validates a array can be written to and read from
func TestDenseQueryWrite(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Create attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute2)

	// Create attribute to add to schema
	attribute3, err := NewAttribute(context, "a3", TILEDB_FLOAT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute3)

	// Create attribute to add to schema
	attribute4, err := NewAttribute(context, "a4", TILEDB_STRING_UTF8)
	require.NoError(t, err)
	assert.NotNil(t, attribute4)

	// Create attribute to add to schema
	attribute5, err := NewAttribute(context, "a5", TILEDB_CHAR)
	require.NoError(t, err)
	assert.NotNil(t, attribute5)

	// Create attribute to add to schema
	attribute6, err := NewAttribute(context, "a6", TILEDB_CHAR)
	require.NoError(t, err)
	assert.NotNil(t, attribute5)

	require.NoError(t, attribute6.SetNullable(true))

	attribute7, err := NewAttribute(context, "a7", TILEDB_CHAR)
	require.NoError(t, err)
	assert.NotNil(t, attribute7)

	require.NoError(t, attribute7.SetNullable(true))

	// Set a7 to be variable length
	require.NoError(t, attribute7.SetCellValNum(TILEDB_VAR_NUM))

	// Set a3 to be variable length
	require.NoError(t, attribute3.SetCellValNum(TILEDB_VAR_NUM))

	// Set a4 to be variable length
	require.NoError(t, attribute4.SetCellValNum(TILEDB_VAR_NUM))

	// Set a5 to be variable length
	require.NoError(t, attribute5.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute, attribute2, attribute3, attribute4, attribute5, attribute6, attribute7))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Open array for writting
	require.NoError(t, array.Open(TILEDB_WRITE))

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.SetSubArray([]int8{0, 1}))

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_ROW_MAJOR))

	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)

	bufferA2 := []byte("ab")
	_, err = query.SetBuffer("a2", bufferA2)
	require.NoError(t, err)

	bufferA3 := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offsetBufferA3 := []uint64{0, 3}
	_, _, err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	require.NoError(t, err)

	bufferA4 := []byte("hello" + "world")
	offsetBufferA4 := []uint64{0, 5}
	// Second byte array so we can compare reads
	bufferA4Comparison := make([]byte, len(bufferA4))
	elementsCopied := copy(bufferA4Comparison, bufferA4)
	assert.Equal(t, len(bufferA4), elementsCopied)

	_, _, err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	require.NoError(t, err)

	bufferA5 := "hello" + "world"
	offsetBufferA5 := []uint64{0, 5}
	// Second string array so we can compare reads
	bufferA5Comparison := make([]byte, len(bufferA5)) // new(string, len(bufferA5))
	elementsCopied = copy(bufferA5Comparison, bufferA5)
	assert.Equal(t, len(bufferA5), elementsCopied)
	assert.EqualValues(t, bufferA5, bufferA5Comparison)
	bufferA5Bytes := []byte(bufferA5)

	_, _, err = query.SetBufferVar("a5", offsetBufferA5, bufferA5Bytes)
	require.NoError(t, err)

	bufferA6 := []byte("ab")
	validityBufferA6 := []uint8{0, 1}
	_, _, err = query.SetBufferNullable("a6", bufferA6, validityBufferA6)
	require.NoError(t, err)

	bufferA6Comparison := make([]byte, len(bufferA6))
	elementsCopied = copy(bufferA6Comparison, bufferA6)
	assert.Equal(t, len(bufferA6), elementsCopied)
	assert.EqualValues(t, bufferA6, bufferA6Comparison)

	bufferA6ValidityComparison := make([]uint8, len(validityBufferA6))
	elementsCopied = copy(bufferA6ValidityComparison, validityBufferA6)
	assert.Equal(t, len(bufferA6), elementsCopied)
	assert.EqualValues(t, bufferA6ValidityComparison, validityBufferA6)

	bufferA7 := "hello" + "world"
	offsetBufferA7 := []uint64{0, 5}
	validityBufferA7 := []uint8{0, 1}
	bufferA7Bytes := []byte(bufferA7)
	_, _, _, err = query.SetBufferVarNullable("a7", offsetBufferA7, bufferA7Bytes, validityBufferA7)
	require.NoError(t, err)

	bufferA7Comparison := make([]byte, len(bufferA7))
	elementsCopied = copy(bufferA7Comparison, bufferA7)
	assert.Equal(t, len(bufferA7), elementsCopied)
	assert.EqualValues(t, bufferA7, bufferA7Comparison)

	bufferA7ValidityComparison := make([]byte, len(validityBufferA7))
	elementsCopied = copy(bufferA7ValidityComparison, validityBufferA7)
	assert.Equal(t, len(validityBufferA7), elementsCopied)
	assert.EqualValues(t, validityBufferA7, bufferA7ValidityComparison)
	require.NoError(t, err)

	// Submit write query
	require.NoError(t, query.Submit())

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	require.NoError(t, query.Finalize())
	// Close and prepare to read
	require.NoError(t, array.Close())

	// Reopen array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	// Get non empty domain, which should not be empty
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	require.NoError(t, err)
	assert.NotNil(t, nonEmptyDomain)
	assert.False(t, isEmpty)
	assert.EqualValues(t, []NonEmptyDomain{{DimensionName: "dim1", Bounds: []int8{0, 1}}}, nonEmptyDomain)

	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set read subarray to only data that was written
	subArray := []int8{0, 1}
	require.NoError(t, query.SetSubArray(subArray))

	bufferElements, err := query.EstimateBufferElements()
	require.NoError(t, err)
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
	require.NoError(t, err)

	readBufferA2 := make([]byte, 2)
	_, err = query.SetBuffer("a2", readBufferA2)
	require.NoError(t, err)

	readBufferA3 := make([]float32, 5)
	readOffsetBufferA3 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a3", readOffsetBufferA3, readBufferA3)
	require.NoError(t, err)

	readBufferA4 := make([]byte, 10)
	readOffsetBufferA4 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a4", readOffsetBufferA4, readBufferA4)
	require.NoError(t, err)

	readBufferA5 := make([]byte, 10) // make(string, 10)
	readOffsetBufferA5 := make([]uint64, 2)
	_, _, err = query.SetBufferVar("a5", readOffsetBufferA5, readBufferA5)
	require.NoError(t, err)

	readBufferA6 := make([]byte, 2)
	readValidityBufferA6 := make([]uint8, 2)
	_, _, err = query.SetBufferNullable("a6", readBufferA6, readValidityBufferA6)
	require.NoError(t, err)

	readBufferA7 := make([]byte, 10)
	readOffsetBufferA7 := make([]uint64, 2)
	readValidityBufferA7 := make([]uint8, 2)
	_, _, _, err = query.SetBufferVarNullable("a7", readOffsetBufferA7, readBufferA7, readValidityBufferA7)
	require.NoError(t, err)

	// Set read layout
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit read query async
	require.NoError(t, query.SubmitAsync())

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		require.NoError(t, err)
		assert.Equal(t, TILEDB_INPROGRESS, status)
	}
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	require.NoError(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	assert.EqualValues(t, bufferA1, readBufferA1)
	assert.EqualValues(t, bufferA2, readBufferA2)
	assert.EqualValues(t, bufferA3, readBufferA3)
	assert.EqualValues(t, bufferA4Comparison, readBufferA4)
	assert.EqualValues(t, bufferA5Comparison, readBufferA5)
	assert.EqualValues(t, bufferA6Comparison, readBufferA6)
	assert.EqualValues(t, bufferA7Comparison, readBufferA7)

	bufferA1InterfaceGet, err := query.Buffer("a1")
	require.NoError(t, err)
	assert.EqualValues(t, bufferA1, bufferA1InterfaceGet.([]int32))

	offsetsBufferA4Get, bufferA4InterfaceGet, err := query.BufferVar("a4")
	require.NoError(t, err)
	assert.EqualValues(t, bufferA4Comparison, bufferA4InterfaceGet.([]byte))
	assert.EqualValues(t, offsetBufferA4, offsetsBufferA4Get)

	offsetsBufferA5Get, bufferA5InterfaceGet, err := query.BufferVar("a5")
	require.NoError(t, err)
	assert.EqualValues(t, bufferA5Comparison, bufferA5InterfaceGet.([]byte))
	assert.EqualValues(t, offsetBufferA5, offsetsBufferA5Get)

	bufferA6InterfaceGet, bufferA6ValidityGet, err := query.BufferNullable("a6")
	require.NoError(t, err)
	assert.EqualValues(t, bufferA6Comparison, bufferA6InterfaceGet.([]byte))
	assert.EqualValues(t, bufferA6ValidityComparison, bufferA6ValidityGet)

	offsetsBufferA7Get, bufferA7InterfaceGet, bufferA7ValidityGet, err := query.BufferVarNullable("a7")
	require.NoError(t, err)
	assert.EqualValues(t, bufferA7Comparison, bufferA7InterfaceGet.([]byte))
	assert.EqualValues(t, offsetBufferA7, offsetsBufferA7Get)
	assert.EqualValues(t, bufferA7ValidityComparison, bufferA7ValidityGet)

	query.Free()
}

// TestSparseQueryDelete validates that sparse array elements can be deleted
func TestSparseQueryDelete(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))

	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Open array for writting
	require.NoError(t, array.Open(TILEDB_WRITE))

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_UNORDERED))

	// Create write buffers
	bufferA1 := []int32{1, 2, 3, 4}
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)

	// Set coordinates, since test is 1d, this is subarray
	subArray := []int8{0, 1, 2, 3}
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	// Submit write query
	require.NoError(t, query.Submit())

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	require.NoError(t, query.Finalize())
	// Close and prepare to delete
	require.NoError(t, array.Close())

	// Prepare a Delete query for elements dim1[2,3]
	// Reopen array for deletion
	require.NoError(t, array.Open(TILEDB_DELETE))

	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	condition, err := NewQueryCondition(context, "dim1", TILEDB_QUERY_CONDITION_GE, int8(2))
	require.NoError(t, err)

	err = query.SetQueryCondition(condition)
	require.NoError(t, err)

	// submit and finalize query
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	// Close and prepare to read
	require.NoError(t, array.Close())

	// Reopen array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set coordinates, since test is 1d, this is subarray
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 4)
	_, err = query.SetBuffer("a1", readBufferA1)
	require.NoError(t, err)

	// Set read layout
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit read query async
	require.NoError(t, query.SubmitAsync())

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		require.NoError(t, err)
		assert.Equal(t, TILEDB_INPROGRESS, status)
	}
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	require.NoError(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	assert.ElementsMatch(t, []int32{1, 2, 0, 0}, readBufferA1)
}

// TestSparseQueryWrite validates a sparse array can be written to and read from
func TestSparseQueryWrite(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension

	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))

	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Open array for writting
	require.NoError(t, array.Open(TILEDB_WRITE))

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_UNORDERED))

	// Create write buffers
	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)

	// Set coordinates, since test is 1d, this is subarray
	subArray := []int8{0, 1}
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	// Submit write query
	require.NoError(t, query.Submit())

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	require.NoError(t, query.Finalize())
	// Close and prepare to read
	require.NoError(t, array.Close())

	// Reopen array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set read subarray to only data that was written
	// err = query.SetSubArray(subArray)
	// require.NoError(t, err)

	// Set coordinates, since test is 1d, this is subarray
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	bufferElements, err := query.EstimateBufferElements()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bufferElements["a1"][0])
	assert.Equal(t, uint64(2), bufferElements["a1"][1])

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 2)
	_, err = query.SetBuffer("a1", readBufferA1)
	require.NoError(t, err)

	// Set read layout
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit read query async
	require.NoError(t, query.SubmitAsync())

	// Wait for status to return complete or to error
	// Loop while status is inprogress
	for status, err = query.Status(); status == TILEDB_INPROGRESS && err == nil; status, err = query.Status() {
		require.NoError(t, err)
		assert.Equal(t, TILEDB_INPROGRESS, status)
	}
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	require.NoError(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	assert.ElementsMatch(t, bufferA1, readBufferA1)
}

// TestSparseQueryWriteNullable validates a sparse array can be written to and read from
func TestSparseQueryWriteNullable(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension

	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set Attribute nullable
	require.NoError(t, attribute.SetNullable(true))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))

	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	// Open array for writting
	require.NoError(t, array.Open(TILEDB_WRITE))

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set write layout
	assert.Nil(t, query.SetLayout(TILEDB_UNORDERED))

	// Create write buffers
	bufferA1 := []int32{1, 2, 3}
	bufferA1Validity := []uint8{1, 1, 0}
	_, _, err = query.SetBufferNullable("a1", bufferA1, bufferA1Validity)
	require.NoError(t, err)

	// Set coordinates, since test is 1d, this is subarray
	subArray := []int8{0, 1, 2}
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	// Submit write query
	require.NoError(t, query.Submit())

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err := query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	// Finalize Write
	require.NoError(t, query.Finalize())
	// Close and prepare to read
	require.NoError(t, array.Close())

	// Reopen array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Set read subarray to only data that was written
	require.NoError(t, query.AddRange(0, 0, 3))

	// Set coordinates, since test is 1d, this is subarray
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)

	bufferElements, err := query.EstimateBufferElements()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bufferElements["a1"][0])
	assert.Equal(t, uint64(3), bufferElements["a1"][1])
	assert.Equal(t, uint64(3), bufferElements["a1"][2])

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 3)
	readBufferA1Validity := make([]uint8, 3)
	_, _, err = query.SetBufferNullable("a1", readBufferA1, readBufferA1Validity)
	require.NoError(t, err)

	// Set read layout
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))

	// Submit read query async
	require.NoError(t, query.Submit())

	status, err = query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// Validate query type
	queryType, err = query.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Results should be returned
	hasResults, err := query.HasResults()
	require.NoError(t, err)
	assert.Equal(t, true, hasResults)

	// Validate read buffers equal original write buffers
	// First two values of A1 should match, and the 3rd is null so don't bother checking
	assert.EqualValues(t, bufferA1[0], readBufferA1[0])
	assert.EqualValues(t, bufferA1[1], readBufferA1[1])
	assert.ElementsMatch(t, bufferA1Validity, readBufferA1Validity)
}

// TestSparseQueryWriteHilbertLayout shows that Hilbert order is not applicable
// to queries queries
func TestSparseQueryWriteHilbertLayout(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(context, "dim1", TILEDB_INT8, []int8{0, 9}, int8(10))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.Check())
	tmpArrayPath := t.TempDir()
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)
	require.NoError(t, array.Create(arraySchema))

	// Write query
	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	bufferA1 := []int32{1, 2}
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)
	subArray := []int8{0, 1}
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)
	// Set write layout
	// Hilbert order not applicable to write queries
	assert.NotNil(t, query.SetLayout(TILEDB_HILBERT))
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	// Read query
	require.NoError(t, array.Open(TILEDB_READ))
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	bufferA1 = make([]int32, 2)
	_, err = query.SetBuffer("a1", bufferA1)
	require.NoError(t, err)
	subArray = make([]int8, 2)
	_, err = query.SetBuffer("dim1", subArray)
	require.NoError(t, err)
	// Set write layout
	// Hilbert order not applicable to write queries
	assert.NotNil(t, query.SetLayout(TILEDB_HILBERT))
	require.NoError(t, query.Finalize())
}

func TestQueryConfig(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim, colDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetAllowsDups(true))

	allowDups, err := arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, true, allowDups)

	require.NoError(t, arraySchema.SetAllowsDups(false))

	// Dense array, allowDups should be false
	allowDups, err = arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, false, allowDups)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 1, 2}
	a1DataWrite := []int32{1, 2, 3}
	a1OffWrite := []uint64{0, 4, 8}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("rows", buffD1)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", buffD2)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	rowsDataSize, err := query.BufferSize("rows")
	require.NoError(t, err)
	assert.Equal(t, len(buffD1), int(rowsDataSize))
	colsDataSize, err := query.BufferSize("cols")
	require.NoError(t, err)
	assert.Equal(t, len(buffD2), int(colsDataSize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

	// Prepare buffers
	rows := make([]int32, 4)
	cols := make([]int32, 4)
	// Allocate 4 bytes to store the read result
	a1DataRead := make([]int32, 12)
	a1OffRead := make([]uint64, 12)

	// Prepare the query
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)

	// Read value at cell 2, 2
	require.NoError(t, query.AddRange(0, 1, 2))
	require.NoError(t, query.AddRange(1, 1, 2))

	// Create configuration
	configQuery, err := NewConfig()
	require.NoError(t, err)

	require.NoError(t, configQuery.Set("sm.var_offsets.mode", "elements"))
	require.NoError(t, query.SetConfig(configQuery))

	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	_, _, err = query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rows)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", cols)
	require.NoError(t, err)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains [3], has size of 4
	assert.EqualValues(t, 12, len(a1DataRead))
	assert.EqualValues(t, 12, len(a1OffRead))

	// Offsets will be element count due to config setting of "sm.var_offsets.mode"
	assert.EqualValues(t, 1, a1OffRead[1])

	query.Free()
}

func TestQueryStats(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create row dimension
	rowDim, err := NewDimension(context, "rows", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, rowDim)

	// Test create row dimension
	colDim, err := NewDimension(context, "cols", TILEDB_INT32, []int32{1, 4}, int32(2))
	require.NoError(t, err)
	assert.NotNil(t, colDim)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	// Add dimensions
	require.NoError(t, domain.AddDimensions(rowDim, colDim))

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	require.NoError(t, arraySchema.SetAllowsDups(true))

	allowDups, err := arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, true, allowDups)

	require.NoError(t, arraySchema.SetAllowsDups(false))

	// Dense array, allowDups should be false
	allowDups, err = arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, false, allowDups)

	require.NoError(t, arraySchema.SetCellOrder(TILEDB_ROW_MAJOR))
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_ROW_MAJOR))

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Set a1 to be variable length
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	// Set Domain
	require.NoError(t, arraySchema.SetDomain(domain))

	// Validate Schema
	require.NoError(t, arraySchema.Check())

	// create temp group name
	tmpArrayPath := t.TempDir()
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	// Prepare some data for the array
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 1, 2}
	a1DataWrite := []byte("abbccc")
	a1OffWrite := []uint64{0, 1, 3}

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_WRITE))
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	require.NoError(t, query.SetLayout(TILEDB_GLOBAL_ORDER))
	_, _, err = query.SetBufferVar("a1", a1OffWrite, a1DataWrite)
	require.NoError(t, err)
	_, err = query.SetBuffer("rows", buffD1)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", buffD2)
	require.NoError(t, err)

	// Check the buffer sizes
	offsetSize, dataSize, err := query.BufferSizeVar("a1")
	require.NoError(t, err)
	assert.Equal(t, len(a1OffWrite), int(offsetSize))
	assert.Equal(t, len(a1DataWrite), int(dataSize))
	rowsDataSize, err := query.BufferSize("rows")
	require.NoError(t, err)
	assert.Equal(t, len(buffD1), int(rowsDataSize))
	colsDataSize, err := query.BufferSize("cols")
	require.NoError(t, err)
	assert.Equal(t, len(buffD2), int(colsDataSize))

	// Perform the write, finalize and close the array.
	require.NoError(t, query.Submit())
	require.NoError(t, query.Finalize())
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))

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
	require.NoError(t, err)
	assert.NotNil(t, query)

	require.NoError(t, query.SetSubArray(subArray))
	require.NoError(t, query.SetLayout(TILEDB_ROW_MAJOR))
	offsetBufferSize, effectiveBufferSize, err := query.SetBufferVar("a1",
		a1OffRead, a1DataRead)
	require.NoError(t, err)
	assert.NotNil(t, query)
	_, err = query.SetBuffer("rows", rows)
	require.NoError(t, err)
	_, err = query.SetBuffer("cols", cols)
	require.NoError(t, err)

	// Submit the query
	require.NoError(t, query.Submit())

	// Data buffer contains "ccc", has size of 4
	assert.EqualValues(t, len(a1DataRead), 4)

	// Only after submit is the *offsetBufferSize available
	// Offset size is expected to be 1*sizeof(uint64)
	assert.EqualValues(t, *offsetBufferSize, 8)

	// Only after submit is the *effectiveBufferSize available
	// "ccc" indeed has effective buffer size of 3
	assert.EqualValues(t, *effectiveBufferSize, 3)

	elements, err := query.ResultBufferElements()
	require.NoError(t, err)
	assert.EqualValues(t, [3]uint64{1, 3, 0}, elements["a1"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["rows"])
	assert.EqualValues(t, [3]uint64{0, 1, 0}, elements["cols"])

	stats, err := query.Stats()
	require.NoError(t, err)
	assert.NotEmpty(t, stats, 0)

	stats, err = query.context.Stats()
	require.NoError(t, err)
	assert.NotEmpty(t, stats, 0)

	query.Free()
}

func TestSetDataBufferUnsafe(t *testing.T) {
	// create a 1d array x[a] with a fixed length int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	dataPtr := unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&dataBuffer)).Data)
	n, err := q.SetDataBufferUnsafe("a", dataPtr, 16)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *n)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	dataPtr = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&dataBuffer)).Data)
	n, err = q.SetDataBufferUnsafe("a", dataPtr, 40)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *n)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	assert.Equal(t, int32(4), dataBuffer[3])
	assert.Equal(t, int32(5), dataBuffer[4])
	assert.Equal(t, int32(6), dataBuffer[5])
	assert.Equal(t, int32(7), dataBuffer[6])

	// verify that GetDataBuffer works for buffers passed unsafe
	storedBuffer, err := q.GetDataBuffer("a")
	require.NoError(t, err)
	storedDataBuffer, ok := storedBuffer.([]int32)
	require.True(t, ok)
	require.Equal(t, uintptr(dataPtr),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedDataBuffer)).Data))
}

func TestGetDataBuffer(t *testing.T) {
	// create a 1d array x[a] with a fixed length int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// create a write query, set the data buffer and read it back
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)

	storedBuffer, err := q.GetDataBuffer("a")
	require.NoError(t, err)
	storedDataBuffer, ok := storedBuffer.([]int32)
	require.True(t, ok)
	require.Equal(t, ((*reflect.SliceHeader)(unsafe.Pointer(&dataBuffer)).Data),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedDataBuffer)).Data))
}

func TestSetDataBuffer(t *testing.T) {
	// create a 1d array x[a] with a fixed length int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	np, err := q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	np, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())
	assert.Equal(t, int32(4), dataBuffer[3])
	assert.Equal(t, int32(5), dataBuffer[4])
	assert.Equal(t, int32(6), dataBuffer[5])
	assert.Equal(t, int32(7), dataBuffer[6])
}

func TestGetExpectedDataBufferLength(t *testing.T) {
	// create a 1d array x[a] with a fixed length int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))

	require.NoError(t, array.Open(TILEDB_READ))
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{0, 0, 0, 0}
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)

	t.Run("ProperQuery", func(t *testing.T) {
		storedBuffer, err := q.GetDataBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := q.GetExpectedDataBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedClientSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, true)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		_, err = dq.SetDataBuffer("a", []int32{0, 0, 0, 0})
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, true)
		require.NoError(t, err)

		storedBuffer, err := dq.GetDataBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := dq.GetExpectedDataBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedServerSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, false)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, false)
		require.NoError(t, err)

		storedBuffer, err := dq.GetDataBuffer("a")
		require.NoError(t, err)
		require.Nil(t, storedBuffer)
		siz, err := dq.GetExpectedDataBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})
}

func TestSetValidityBufferUnsafe(t *testing.T) {
	// create a 1d array x[a] with a fixed length nullable int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, attribute.SetNullable(true))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	validityBuffer := []uint8{1, 1, 0, 1}
	validityPtr := unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&validityBuffer)).Data)
	n, err := q.SetValidityBufferUnsafe("a", validityPtr, 4)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Equal(t, uint64(len(validityBuffer))*uint64(unsafe.Sizeof(validityBuffer[0])), *n)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	validityBuffer = []uint8{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	validityPtr = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&validityBuffer)).Data)
	n, err = q.SetValidityBufferUnsafe("a", validityPtr, 10)
	require.NoError(t, err)
	require.NotNil(t, n)
	require.Equal(t, uint64(len(validityBuffer))*uint64(unsafe.Sizeof(validityBuffer[0])), *n)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	assert.Equal(t, int32(4), dataBuffer[3])
	assert.Equal(t, int32(5), dataBuffer[4])
	assert.Equal(t, int32(6), dataBuffer[5])
	assert.Equal(t, int32(7), dataBuffer[6])

	// verify that GetDataBuffer works for buffers passed unsafe
	storedValidityBuffer, err := q.GetValidityBuffer("a")
	require.NoError(t, err)
	require.Equal(t, uintptr(validityPtr),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedValidityBuffer)).Data))
}

func TestGetValidityBuffer(t *testing.T) {
	// create a 1d array x[a] with a fixed length nullable int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, attribute.SetNullable(true))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// create a write query, set the validity buffer and read it back
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	validityBuffer := []uint8{1, 1, 0, 1}
	_, err = q.SetValidityBuffer("a", validityBuffer)
	require.NoError(t, err)

	storedValidityBuffer, err := q.GetValidityBuffer("a")
	require.NoError(t, err)
	require.Equal(t, ((*reflect.SliceHeader)(unsafe.Pointer(&validityBuffer)).Data),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedValidityBuffer)).Data))
}

func TestSetValidityBuffer(t *testing.T) {
	// create a 1d array x[a] with a fixed length nullable int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, attribute.SetNullable(true))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	np, err := q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	validityBuffer := []uint8{1, 1, 0, 1}
	vnp, err := q.SetValidityBuffer("a", validityBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(validityBuffer))*uint64(unsafe.Sizeof(validityBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = []int32{0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	np, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	validityBuffer = []uint8{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}
	vnp, err = q.SetValidityBuffer("a", validityBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(validityBuffer))*uint64(unsafe.Sizeof(validityBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())
	assert.Equal(t, []uint8{0, 0, 0, 1, 1, 0, 1, 0, 0, 0}, validityBuffer)
}

func TestGetExpectedValidityBufferLength(t *testing.T) {
	// create a 1d array x[a] with a fixed length nullable int32 attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, attribute.SetNullable(true))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []int32{4, 5, 6, 7}
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	validityBuffer := []uint8{1, 1, 0, 1}
	_, err = q.SetValidityBuffer("a", validityBuffer)
	require.NoError(t, err)

	t.Run("ProperQuery", func(t *testing.T) {
		storedBuffer, err := q.GetValidityBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := q.GetExpectedValidityBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedClientSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, true)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		_, err = dq.SetDataBuffer("a", []int32{0, 0, 0, 0})
		require.NoError(t, err)
		_, err = dq.SetValidityBuffer("a", []uint8{0, 0, 0, 0})
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, true)
		require.NoError(t, err)

		storedBuffer, err := dq.GetValidityBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := dq.GetExpectedValidityBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedServerSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, false)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, false)
		require.NoError(t, err)

		storedBuffer, err := dq.GetValidityBuffer("a")
		require.NoError(t, err)
		require.Nil(t, storedBuffer)
		siz, err := dq.GetExpectedValidityBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})
}

func TestSetOffsetsBufferUnsafe(t *testing.T) {
	// create a 1d array x[a] with a var length attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []byte("HelloWorldFromTiledb")
	np, err := q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	offsetsBuffer := []uint64{0, 5, 10, 14}
	offsetsPtr := unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&offsetsBuffer)).Data)
	vnp, err := q.SetOffsetsBufferUnsafe("a", offsetsPtr, 32)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(offsetsBuffer))*uint64(unsafe.Sizeof(offsetsBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = make([]byte, 40)
	np, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	offsetsBuffer = make([]uint64, 10)
	offsetsPtr = unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&offsetsBuffer)).Data)
	vnp, err = q.SetOffsetsBufferUnsafe("a", offsetsPtr, 80)
	require.NoError(t, err)
	require.NotNil(t, vnp)
	require.Equal(t, uint64(len(offsetsBuffer))*uint64(unsafe.Sizeof(offsetsBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	assert.Equal(t, []uint64{0, 1, 2, 3, 8, 13, 17, 23, 24, 25}, offsetsBuffer)

	// verify that GetOffsetsBuffer works for buffers passed unsafe
	storedOffsetsBuffer, err := q.GetOffsetsBuffer("a")
	require.NoError(t, err)
	require.Equal(t, uintptr(offsetsPtr),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedOffsetsBuffer)).Data))
}

func TestGetOffsetsBuffer(t *testing.T) {
	// create a 1d array x[a] with a var length attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// create a write query, set the validity buffer and read it back
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	offsetsBuffer := []uint64{0, 2, 4, 8}
	_, err = q.SetOffsetsBuffer("a", offsetsBuffer)
	require.NoError(t, err)

	storedOffsetsBuffer, err := q.GetOffsetsBuffer("a")
	require.NoError(t, err)
	require.Equal(t, ((*reflect.SliceHeader)(unsafe.Pointer(&offsetsBuffer)).Data),
		((*reflect.SliceHeader)(unsafe.Pointer(&storedOffsetsBuffer)).Data))
}

func TestSetOffsetsBuffer(t *testing.T) {
	// create a 1d array x[a] with a var length attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	// open the array and write a slice
	require.NoError(t, array.Open(TILEDB_WRITE))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := []byte("HelloWorldFromTiledb")
	np, err := q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	offsetsBuffer := []uint64{0, 5, 10, 14}
	vnp, err := q.SetOffsetsBuffer("a", offsetsBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(offsetsBuffer))*uint64(unsafe.Sizeof(offsetsBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err := q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())

	// open the array to read the full array and verify the written cells
	require.NoError(t, array.Open(TILEDB_READ))
	tdbCtx, err = NewContext(config)
	require.NoError(t, err)
	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 1, 10))
	dataBuffer = make([]byte, 40)
	np, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(dataBuffer))*uint64(unsafe.Sizeof(dataBuffer[0])), *np)
	offsetsBuffer = make([]uint64, 10)
	vnp, err = q.SetOffsetsBuffer("a", offsetsBuffer)
	require.NoError(t, err)
	require.NotNil(t, np)
	require.Equal(t, uint64(len(offsetsBuffer))*uint64(unsafe.Sizeof(offsetsBuffer[0])), *vnp)
	require.NoError(t, q.Submit())
	status, err = q.Status()
	require.NoError(t, err)
	require.Equal(t, TILEDB_COMPLETED, status)
	require.NoError(t, array.Close())
	assert.Equal(t, []uint64{0, 1, 2, 3, 8, 13, 17, 23, 24, 25}, offsetsBuffer)
}

func TestGetExpectedOffsetsBufferLength(t *testing.T) {
	// create a 1d array x[a] with a var length attribute
	config, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(config)
	require.NoError(t, err)
	dimension, err := NewDimension(tdbCtx, "x", TILEDB_INT8, []int8{1, 10}, int8(5))
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension))
	arraySchema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	attribute, err := NewAttribute(tdbCtx, "a", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	require.NoError(t, attribute.SetCellValNum(TILEDB_VAR_NUM))
	require.NoError(t, arraySchema.AddAttributes(attribute))
	require.NoError(t, arraySchema.SetDomain(domain))
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(arraySchema))
	require.NoError(t, array.Close())

	require.NoError(t, array.Open(TILEDB_READ))
	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)
	require.NoError(t, q.AddRangeByName("x", 4, 7))
	dataBuffer := make([]byte, 40)
	_, err = q.SetDataBuffer("a", dataBuffer)
	require.NoError(t, err)
	offsetsBuffer := []uint64{0, 0, 0, 0}
	_, err = q.SetOffsetsBuffer("a", offsetsBuffer)
	require.NoError(t, err)

	t.Run("ProperQuery", func(t *testing.T) {
		storedBuffer, err := q.GetOffsetsBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := q.GetExpectedOffsetsBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedClientSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, true)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		_, err = dq.SetDataBuffer("a", make([]byte, 40))
		require.NoError(t, err)
		_, err = dq.SetOffsetsBuffer("a", []uint64{0, 0, 0, 0})
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, true)
		require.NoError(t, err)

		storedBuffer, err := dq.GetOffsetsBuffer("a")
		require.NoError(t, err)
		require.NotNil(t, storedBuffer)
		siz, err := dq.GetExpectedOffsetsBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})

	t.Run("SerializedServerSideQuery", func(t *testing.T) {
		bf, err := SerializeQuery(q, TILEDB_CAPNP, false)
		require.NoError(t, err)
		require.NotNil(t, bf)
		buf, err := bf.Flatten()
		require.NoError(t, err)

		dq, err := NewQuery(tdbCtx, array)
		require.NoError(t, err)
		err = DeserializeQuery(dq, buf, TILEDB_CAPNP, false)
		require.NoError(t, err)

		storedBuffer, err := dq.GetOffsetsBuffer("a")
		require.NoError(t, err)
		require.Nil(t, storedBuffer)
		siz, err := dq.GetExpectedOffsetsBufferLength("a")
		require.NoError(t, err)
		require.Equal(t, uint64(4), siz)
	})
}
