package tiledb

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, 5)
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

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Crete attribute to add to schema
	attribute2, err := NewAttribute(context, "a2", TILEDB_STRING_ASCII)
	require.NoError(t, err)
	assert.NotNil(t, attribute2)

	require.NoError(t, attribute2.SetCellValNum(TILEDB_VAR_NUM))

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute, attribute2))

	require.NoError(t, arraySchema.SetDomain(domain))

	return arraySchema
}

func TestArray(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// create temp group name
	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
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

	//err = array.Consolidate()
	//require.NoError(t, err)

	// Open array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	// Test re-opening
	require.NoError(t, array.Reopen())

	// Close Array
	require.NoError(t, array.Close())

	// Open array for reading At
	require.NoError(t, array.OpenWithOptions(TILEDB_READ, WithEndTimestamp(uint64(time.Now().UnixNano()/1000000))))

	// Get the array schema
	arraySchema, err = array.Schema()
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Validate array schema is usable
	tileOrder, err := arraySchema.TileOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_ROW_MAJOR, tileOrder)

	queryType, err := array.QueryType()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Get non empty domain, which is none since no data has been written
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	require.NoError(t, err)
	assert.Nil(t, nonEmptyDomain)
	assert.True(t, isEmpty)

	// Test from name
	nonEmptyDomainFromName, isEmpty, err := array.NonEmptyDomainFromName("dim1")
	require.NoError(t, err)
	assert.Nil(t, nonEmptyDomainFromName)
	assert.True(t, isEmpty)

	// Test from index
	nonEmptyDomainFromIndex, isEmpty, err := array.NonEmptyDomainFromIndex(0)
	require.NoError(t, err)
	assert.Nil(t, nonEmptyDomainFromIndex)
	assert.True(t, isEmpty)

	// Close the array
	require.NoError(t, array.Close())

	arraySchemaLoaded, err := LoadArraySchema(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, arraySchemaLoaded)

	array.Free()
}

func TestArrayEncryption(t *testing.T) {
	encryption_key := "unittestunittestunittestunittest"
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	err = config.Set("sm.encryption_type", TILEDB_AES_256_GCM.String())
	assert.Nil(t, err)

	err = config.Set("sm.encryption_key", encryption_key)
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// create temp group name
	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")
	// Cleanup group when test ends
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}
	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))
	assert.Nil(t, err)

	//err = array.Consolidate()
	//require.NoError(t, err)

	// Open array for reading
	require.NoError(t, array.Open(TILEDB_READ))

	// Test re-opening
	require.NoError(t, array.Reopen())

	// Close Array
	require.NoError(t, array.Close())

	// Open array for reading At
	require.NoError(t, array.OpenWithOptions(TILEDB_READ, WithEndTimestamp(uint64(time.Now().UnixNano()/1000000))))

	// Get the array schema
	arraySchema, err = array.Schema()
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	// Validate array schema is usable
	tileOrder, err := arraySchema.TileOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_ROW_MAJOR, tileOrder)

	queryType, err := array.QueryType()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	// Get non empty domain, which is none since no data has been written
	nonEmptyDomain, isEmpty, err := array.NonEmptyDomain()
	require.NoError(t, err)
	assert.Nil(t, nonEmptyDomain)
	assert.True(t, isEmpty)

	// Close the array
	require.NoError(t, array.Close())

	arraySchemaLoaded, err := LoadArraySchema(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, arraySchemaLoaded)

	array.Free()
}

func TestArray_OpenWithOptions(t *testing.T) {
	t.Run("StartTime", func(t *testing.T) {
		startTime := uint64(1621976364000)
		a, cleanup, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}
		defer cleanup()
		err = a.OpenWithOptions(TILEDB_READ, WithStartTimestamp(startTime))
		assert.NoError(t, err)

		got, err := a.OpenStartTimestamp()
		assert.NoError(t, err)

		assert.Equal(t, startTime, got)
	})

	t.Run("EndTime", func(t *testing.T) {
		endTime := uint64(1621976364666)
		a, cleanup, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}
		defer cleanup()
		err = a.OpenWithOptions(TILEDB_READ, WithEndTimestamp(endTime))
		assert.NoError(t, err)

		got, err := a.OpenEndTimestamp()
		assert.NoError(t, err)

		assert.Equal(t, endTime, got)
	})
}

func newTestArray(t *testing.T) (*Array, func(), error) {
	// Create configuration
	config, err := NewConfig()
	if err != nil {
		return nil, nil, err
	}

	// Test context with config
	context, err := NewContext(config)
	if err != nil {
		return nil, nil, err
	}

	// create temp group name
	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")

	array, err := NewArray(context, tmpArrayPath)
	if err != nil {
		return nil, nil, err
	}

	arraySchema := buildArraySchema(context, t)
	// Create array on disk
	err = array.Create(arraySchema)
	if err != nil {
		return nil, nil, err
	}

	// Create new array struct
	return array, func() {
		// Cleanup group when test ends
		os.RemoveAll(tmpArrayPath)
		if _, err = os.Stat(tmpArrayPath); err == nil {
			os.RemoveAll(tmpArrayPath)
		}
	}, nil
}
