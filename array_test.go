package tiledb

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ExampleNewArray() {
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
	dimension, err := NewDimension(context, "test", []int32{1, 10}, 5)
	if err != nil {
		// Handle error
		return
	}

	// Create Domain
	domain, err := NewDomain(context)
	if err != nil {
		// Handle error
		return
	}

	// Add dimension to domain
	err = domain.AddDimensions(dimension)
	if err != nil {
		// Handle error
		return
	}

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

	err = arraySchema.AddAttributes(attribute)
	if err != nil {
		// Handle error
		return
	}

	array, err := NewArray(context, "my_array")
	if err != nil {
		// Handle error
		return
	}

	err = array.Create(arraySchema)
	if err != nil {
		// Handle error
		return
	}
}

// TestArray tests creating a new dimension
func buildArraySchema(context *Context, t *testing.T) *ArraySchema {
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

	err = attribute2.SetCellValNum(TILEDB_VAR_NUM)
	assert.Nil(t, err)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute, attribute2)
	assert.Nil(t, err)

	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	return arraySchema
}

func TestArray(t *testing.T) {
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

	//err = array.Consolidate()
	//assert.Nil(t, err)

	// Open array for reading
	err = array.Open(TILEDB_READ)
	assert.Nil(t, err)

	// Test re-opening
	err = array.Reopen()
	assert.Nil(t, err)

	// Close Array
	err = array.Close()
	assert.Nil(t, err)

	// Open array for reading At
	err = array.OpenAt(TILEDB_READ, uint64(time.Now().UnixNano()/1000000))
	assert.Nil(t, err)

	// Get the array schema
	arraySchema, err = array.Schema()
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Validate array schema is usable
	tileOrder, err := arraySchema.TileOrder()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_ROW_MAJOR, tileOrder)

	queryType, err := array.QueryType()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Get non empty domain, which is none since no data has been written
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	assert.Nil(t, err)
	assert.NotNil(t, nonEmptyDomain)
	assert.True(t, isEmpty)
	//assert.EqualValues(t, []map[string]interface{}{{"dim1": []int8{1, 10}}}, nonEmptyDomain)

	// Get MaxBufferSize, which is 0 because array is empty
	maxBufferSize, err := array.MaxBufferSize("a1", []int8{1, 6})
	assert.Nil(t, err)
	assert.Zero(t, maxBufferSize)

	// Get MaxBufferSizeVar, which is 0 because array is empty
	maxBufferOffSize, maxBufferValSize, err := array.MaxBufferSizeVar("a2", []int8{1, 6})
	assert.Nil(t, err)
	assert.Zero(t, maxBufferOffSize)
	assert.Zero(t, maxBufferValSize)

	// Close the array
	err = array.Close()
	assert.Nil(t, err)

	arraySchemaLoaded, err := LoadArraySchema(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchemaLoaded)

	array.Free()
}

func TestArrayEncryption(t *testing.T) {
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

	//err = array.Consolidate()
	//assert.Nil(t, err)

	// Open array for reading
	err = array.OpenWithKey(TILEDB_READ, TILEDB_AES_256_GCM, key)
	assert.Nil(t, err)

	// Test re-opening
	err = array.Reopen()
	assert.Nil(t, err)

	// Close Array
	err = array.Close()
	assert.Nil(t, err)

	// Open array for reading At
	err = array.OpenAtWithKey(TILEDB_READ, TILEDB_AES_256_GCM, key, uint64(time.Now().UnixNano()/1000000))
	assert.Nil(t, err)

	// Get the array schema
	arraySchema, err = array.Schema()
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Validate array schema is usable
	tileOrder, err := arraySchema.TileOrder()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_ROW_MAJOR, tileOrder)

	queryType, err := array.QueryType()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Get non empty domain, which is none since no data has been written
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	assert.Nil(t, err)
	assert.NotNil(t, nonEmptyDomain)
	assert.True(t, isEmpty)
	//assert.EqualValues(t, []map[string]interface{}{{"dim1": []int8{1, 10}}}, nonEmptyDomain)

	// Get MaxBufferSize, which is 0 because array is empty
	maxBufferSize, err := array.MaxBufferSize("a1", []int8{1, 6})
	assert.Nil(t, err)
	assert.Zero(t, maxBufferSize)

	// Get MaxBufferSizeVar, which is 0 because array is empty
	maxBufferOffSize, maxBufferValSize, err := array.MaxBufferSizeVar("a2", []int8{1, 6})
	assert.Nil(t, err)
	assert.Zero(t, maxBufferOffSize)
	assert.Zero(t, maxBufferValSize)

	// Close the array
	err = array.Close()
	assert.Nil(t, err)

	arraySchemaLoaded, err := LoadArraySchemaWithKey(context, tmpArrayPath, TILEDB_AES_256_GCM, key)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchemaLoaded)

	array.Free()
}
