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
	err = query.SetBuffer("a1", bufferA1)
	if err != nil {
		// Handle error
		return
	}

	bufferA2 := []byte("ab")
	err = query.SetBuffer("a2", bufferA2)
	if err != nil {
		// Handle error
		return
	}

	bufferA3 := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offsetBufferA3 := []uint64{0, 3}
	err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	if err != nil {
		// Handle error
		return
	}

	bufferA4 := []byte("hello" + "world")
	offsetBufferA4 := []uint64{0, 5}
	err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
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
	err = query.SetBuffer("a1", readBufferA1)
	if err != nil {
		// Handle error
		return
	}

	readBufferA2 := make([]byte, 2)
	err = query.SetBuffer("a2", readBufferA2)
	if err != nil {
		// Handle error
		return
	}

	readBufferA3 := make([]float32, 5)
	readOffsetBufferA3 := make([]uint64, 2)
	err = query.SetBufferVar("a3", readOffsetBufferA3, readBufferA3)
	if err != nil {
		// Handle error
		return
	}
	readBufferA4 := make([]byte, 10)
	readOffsetBufferA4 := make([]uint64, 2)
	err = query.SetBufferVar("a4", readOffsetBufferA4, readBufferA4)
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
	err = query.SetBuffer("a1", bufferA1Bad)
	assert.NotNil(t, err)

	// Create read buffers
	bufferA1 := make([]int32, 2)
	err = query.SetBuffer("a1", bufferA1)
	assert.Nil(t, err)

	bufferA2 := make([]byte, 2)
	err = query.SetBuffer("a2", bufferA2)
	assert.Nil(t, err)

	bufferA3 := make([]float32, 5)
	offsetBufferA3 := make([]uint64, 3)
	err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	assert.Nil(t, err)

	bufferA4 := make([]byte, 4)
	offsetBufferA4 := make([]uint64, 4)
	err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
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
	bufferA1 := []int32{1, 2}
	err = query.SetBuffer("a1", bufferA1)
	assert.Nil(t, err)

	bufferA2 := []byte("ab")
	err = query.SetBuffer("a2", bufferA2)
	assert.Nil(t, err)

	bufferA3 := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	offsetBufferA3 := []uint64{0, 3}
	err = query.SetBufferVar("a3", offsetBufferA3, bufferA3)
	assert.Nil(t, err)

	bufferA4 := []byte("hello" + "world")
	offsetBufferA4 := []uint64{0, 5}
	// Second byte array so we can compare reads
	bufferA4Comparison := make([]byte, len(bufferA4))
	elementsCopied := copy(bufferA4Comparison, bufferA4)
	assert.Equal(t, len(bufferA4), elementsCopied)

	err = query.SetBufferVar("a4", offsetBufferA4, bufferA4)
	// Immediately set bufferA4 to nil to validate underlying array is not GC'ed
	bufferA4 = nil
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
	subArray := []int8{0, 1}
	err = query.SetSubArray(subArray)
	assert.Nil(t, err)

	maxElements, err := array.MaxBufferElements(subArray)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), maxElements["a1"][0])
	assert.Equal(t, uint64(2), maxElements["a1"][1])
	assert.Equal(t, uint64(0), maxElements["a2"][0])
	assert.Equal(t, uint64(2), maxElements["a2"][1])
	assert.Equal(t, uint64(2), maxElements["a3"][0])
	assert.Equal(t, uint64(15), maxElements["a3"][1])
	assert.Equal(t, uint64(2), maxElements["a4"][0])
	assert.Equal(t, uint64(20), maxElements["a4"][1])

	// Set empty buffers for reading
	readBufferA1 := make([]int32, 2)
	err = query.SetBuffer("a1", readBufferA1)
	assert.Nil(t, err)

	readBufferA2 := make([]byte, 2)
	err = query.SetBuffer("a2", readBufferA2)
	assert.Nil(t, err)

	readBufferA3 := make([]float32, 5)
	readOffsetBufferA3 := make([]uint64, 2)
	err = query.SetBufferVar("a3", readOffsetBufferA3, readBufferA3)
	assert.Nil(t, err)

	readBufferA4 := make([]byte, 10)
	readOffsetBufferA4 := make([]uint64, 2)
	err = query.SetBufferVar("a4", readOffsetBufferA4, readBufferA4)
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

	query.Free()
}
