package tiledb

import (
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

	// Create attribute to add to schema
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

	err = arraySchema.SetDomain(domain)
	if err != nil {
		// Handle error
		return
	}

	err = CreateArray(context, "my_array", arraySchema)
	if err != nil {
		// Handle error
		return
	}
}

// TestArray tests creating a new dimension
func buildArraySchema(context *Context, t testing.TB) *ArraySchema {
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
	tmpArrayPath := t.TempDir()
	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	require.NoError(t, CreateArray(context, tmpArrayPath, arraySchema))

	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

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
	require.NoError(t, array.OpenWithOptions(TILEDB_READ, WithEndTime(time.Now())))

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
	tmpArrayPath := t.TempDir()
	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	require.NoError(t, CreateArray(context, tmpArrayPath, arraySchema))
	assert.Nil(t, err)

	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

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
		startTime := time.Date(2021, 5, 25, 20, 59, 24, 0, time.UTC)
		a, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}
		err = a.OpenWithOptions(TILEDB_READ, WithStartTime(startTime))
		assert.NoError(t, err)

		got, err := a.OpenStartTime()
		assert.NoError(t, err)

		assert.Equal(t, startTime, got)
	})

	t.Run("EndTime", func(t *testing.T) {
		endTime := time.Date(2021, 5, 25, 20, 59, 24, 666000000, time.UTC)
		a, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}
		err = a.OpenWithOptions(TILEDB_READ, WithEndTime(endTime))
		assert.NoError(t, err)

		got, err := a.OpenEndTime()
		assert.NoError(t, err)

		assert.Equal(t, endTime, got)
	})
}

func TestArray_Metadata(t *testing.T) {
	testKey := "test"
	t.Run("ascii", func(t *testing.T) {
		a, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}

		testString := "abc"
		err = a.Open(TILEDB_WRITE)
		assert.NoError(t, err)

		err = a.PutMetadata(testKey, testString)
		assert.NoError(t, err)

		err = a.Close()
		assert.NoError(t, err)

		err = a.Open(TILEDB_READ)
		assert.NoError(t, err)

		dataType, valNum, value, err := a.GetMetadata(testKey)
		assert.NoError(t, err)
		assert.Equal(t, TILEDB_STRING_UTF8, dataType)
		assert.EqualValues(t, len(testString), valNum)
		assert.Equal(t, testString, value.(string))

		a.Close()
		assert.NoError(t, err)
	})

	t.Run("utf8", func(t *testing.T) {
		a, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}

		testString := "€"
		err = a.Open(TILEDB_WRITE)
		assert.NoError(t, err)

		err = a.PutMetadata(testKey, testString)
		assert.NoError(t, err)

		err = a.Close()
		assert.NoError(t, err)

		err = a.Open(TILEDB_READ)
		assert.NoError(t, err)

		dataType, valNum, value, err := a.GetMetadata(testKey)
		assert.NoError(t, err)
		assert.Equal(t, TILEDB_STRING_UTF8, dataType)
		assert.EqualValues(t, len(testString), valNum)
		assert.Equal(t, testString, value.(string))

		a.Close()
		assert.NoError(t, err)
	})

	t.Run("nulls", func(t *testing.T) {
		a, err := newTestArray(t)
		if err != nil {
			t.Fatalf("failed to create new test array: %v", err)
		}

		testString := []byte("\000\000\000")
		err = a.Open(TILEDB_WRITE)
		assert.NoError(t, err)

		err = arrayPutMetadata(a, TILEDB_STRING_UTF8, testKey, slicePtr(testString), len(testString))
		assert.NoError(t, err)

		err = a.Close()
		assert.NoError(t, err)

		err = a.Open(TILEDB_READ)
		assert.NoError(t, err)

		dataType, valNum, value, err := a.GetMetadata(testKey)
		assert.NoError(t, err)
		assert.Equal(t, TILEDB_STRING_UTF8, dataType)
		assert.EqualValues(t, len(testString), valNum)
		assert.Equal(t, string(testString), value.(string))

		a.Close()
		assert.NoError(t, err)
	})

	t.Run("empty", func(t *testing.T) {
		a, err := newTestArray(t)
		require.NoError(t, err)
		require.NoError(t, a.Open(TILEDB_WRITE))
		empty := []byte{}
		require.NoError(t, arrayPutMetadata(a, TILEDB_STRING_UTF8, testKey, slicePtr(empty), 0))
		require.NoError(t, a.Close())
		require.NoError(t, a.Open(TILEDB_READ))
		dataType, valNum, value, err := a.GetMetadata(testKey)
		require.NoError(t, err)
		assert.Equal(t, TILEDB_STRING_UTF8, dataType)
		assert.EqualValues(t, 1, valNum)
		assert.Equal(t, "", value.(string))
	})
}

func TestDeleteFragments(t *testing.T) {
	// Create an array with domain [1, 10].
	// Create fragments [1,2] [3,4] [5,6] [7,8] [9,10]
	// Delete the first 2 fragments and verify the non empty domain is [5, 10]

	// create an array and write 5 fragments
	array, err := newTestArray(t)
	require.NoError(t, err)

	testStarted := time.Now()
	var fragmentCreatedAt []time.Time

	context, err := NewContext(nil)
	require.NoError(t, err)
	require.NoError(t, array.Open(TILEDB_WRITE))
	for i := 1; i <= 10; i += 2 {
		time.Sleep(100 * time.Millisecond) // give fragments some time distance

		query, err := NewQuery(context, array)
		require.NoError(t, err)
		require.NotNil(t, query)

		subarray, err := array.NewSubarray()
		require.NoError(t, err)
		require.NotNil(t, subarray)

		err = subarray.AddRangeByName("dim1", MakeRange(int8(i), int8(i+1)))
		require.NoError(t, err)
		err = query.SetSubarray(subarray)
		require.NoError(t, err)
		_, err = query.SetDataBuffer("a1", []int32{int32(i), int32(i + 1)})
		require.NoError(t, err)
		_, err = query.SetDataBuffer("a2", []byte("aa"))
		require.NoError(t, err)
		_, err = query.SetOffsetsBuffer("a2", []uint64{0, 1})
		require.NoError(t, err)

		err = query.Submit()
		require.NoError(t, err)

		status, err := query.Status()
		require.NoError(t, err)
		assert.Equal(t, TILEDB_COMPLETED, status)

		fragmentCreatedAt = append(fragmentCreatedAt, time.Now())
	}
	err = array.Close()
	require.NoError(t, err)

	// delete the first two fragments
	uri, err := array.URI()
	require.NoError(t, err)
	err = DeleteFragments(context, uri, uint64(testStarted.UnixMilli()), uint64(fragmentCreatedAt[1].UnixMilli()))
	require.NoError(t, err)

	// verify deletion
	err = array.Open(TILEDB_READ)
	require.NoError(t, err)
	domain, _, err := array.NonEmptyDomainFromName("dim1")
	require.NoError(t, err)
	bounds := domain.Bounds.([]int8)
	require.Equal(t, int8(5), bounds[0])
	require.Equal(t, int8(10), bounds[1])
}

func TestDeleteFragmentsList(t *testing.T) {
	// Create an array with domain [1, 10].
	// Create fragments [1,2] [3,4] [5,6] [7,8] [9,10]
	// Delete the first 2 fragments and verify the others

	// create an array and write 5 fragments
	array, err := newTestArray(t)
	require.NoError(t, err)

	context, err := NewContext(nil)
	require.NoError(t, err)
	err = array.Open(TILEDB_WRITE)
	require.NoError(t, err)
	for i := 1; i <= 10; i += 2 {
		time.Sleep(100 * time.Millisecond)

		query, err := NewQuery(context, array)
		require.NoError(t, err)
		require.NotNil(t, query)

		subarray, err := array.NewSubarray()
		require.NoError(t, err)
		require.NotNil(t, subarray)

		err = subarray.AddRangeByName("dim1", MakeRange(int8(i), int8(i+1)))
		require.NoError(t, err)

		err = query.SetSubarray(subarray)
		require.NoError(t, err)
		_, err = query.SetDataBuffer("a1", []int32{int32(i), int32(i + 1)})
		require.NoError(t, err)
		_, err = query.SetDataBuffer("a2", []byte("aa"))
		require.NoError(t, err)
		_, err = query.SetOffsetsBuffer("a2", []uint64{0, 1})
		require.NoError(t, err)

		err = query.Submit()
		require.NoError(t, err)

		status, err := query.Status()
		require.NoError(t, err)
		assert.Equal(t, TILEDB_COMPLETED, status)
	}
	err = array.Close()
	require.NoError(t, err)

	uri, err := array.URI()
	require.NoError(t, err)

	getFragments := func() []string {
		var fragmentURIs []string
		fragmentInfo, err := NewFragmentInfo(context, uri)
		require.NoError(t, err)
		err = fragmentInfo.Load()
		require.NoError(t, err)
		fragmentNum, err := fragmentInfo.GetFragmentNum()
		require.NoError(t, err)
		for i := uint32(0); i < fragmentNum; i++ {
			uri, err := fragmentInfo.GetFragmentURI(i)
			require.NoError(t, err)
			fragmentURIs = append(fragmentURIs, uri)
		}
		return fragmentURIs
	}
	fragmentURIsInitial := getFragments()

	// delete the first two fragments
	err = DeleteFragmentsList(context, uri, fragmentURIsInitial[0:2])
	require.NoError(t, err)

	// verify deletion
	fragmentURIsAfter := getFragments()
	require.ElementsMatch(t, fragmentURIsInitial[2:], fragmentURIsAfter)
}

func newTestArray(t *testing.T) (*Array, error) {
	// Create configuration
	config, err := NewConfig()
	if err != nil {
		return nil, err
	}

	// Test context with config
	context, err := NewContext(config)
	if err != nil {
		return nil, err
	}

	// create temp group name
	tmpArrayPath := t.TempDir()

	arraySchema := buildArraySchema(context, t)
	// Create array on disk
	err = CreateArray(context, tmpArrayPath, arraySchema)
	if err != nil {
		return nil, err
	}

	array, err := NewArray(context, tmpArrayPath)
	if err != nil {
		return nil, err
	}

	return array, nil
}
