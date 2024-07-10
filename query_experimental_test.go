package tiledb

import (
	"bytes"
	"encoding/json"
	"html/template"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryStatusDetails(t *testing.T) {
	// Create an array

	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Create context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Create dimension
	dimension, err := NewDimension(context, "x", TILEDB_INT8, []int8{0, 9}, int8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Create domain
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
	attribute, err := NewAttribute(context, "v", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Add attribute to schema
	err = arraySchema.AddAttributes(attribute)
	require.NoError(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	require.NoError(t, err)

	// Validate Schema
	err = arraySchema.Check()
	require.NoError(t, err)

	// Create array on disk
	tmpArrayPath := t.TempDir()
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)
	err = array.Create(arraySchema)
	require.NoError(t, err)

	// Write to array

	// Open array for writing
	err = array.Open(TILEDB_WRITE)
	require.NoError(t, err)

	// Create subarray
	subarray, err := array.NewSubarray()
	require.NoError(t, err)
	assert.NotNil(t, subarray)
	err = subarray.SetSubArray([]int8{0, 9})
	require.NoError(t, err)

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	err = query.SetSubarray(subarray)
	require.NoError(t, err)

	// Initialize the data buffer
	bufferV := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, err = query.SetDataBuffer("v", bufferV)
	require.NoError(t, err)

	// Submit write query
	err = query.Submit()
	require.NoError(t, err)

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// close array
	err = array.Close()
	require.NoError(t, err)

	// Read from the array. We will test an incomplete query
	// Open array for reading
	err = array.Open(TILEDB_READ)
	require.NoError(t, err)

	// Create subarray
	subarray, err = array.NewSubarray()
	require.NoError(t, err)
	assert.NotNil(t, subarray)
	err = subarray.SetSubArray([]int8{0, 9}) // we want to read the whole array, 2 full tiles
	require.NoError(t, err)

	// Create read query
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	err = query.SetSubarray(subarray)
	require.NoError(t, err)

	// Initialize the data buffer
	// The buffer should be large enough for 1 tile but not for 2. Tile size is 5
	bufferV = []int32{0, 0, 0, 0, 0, 0}
	_, err = query.SetDataBuffer("v", bufferV)
	require.NoError(t, err)

	// Submit read query
	err = query.Submit()
	require.NoError(t, err)

	// verify query status
	status, err = query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INCOMPLETE, status)

	// verify status details
	details, err := query.StatusDetails()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_REASON_USER_BUFFER_SIZE, details.IncompleteReason)

	// check that the first tile was returned
	assert.Equal(t, int32(1), bufferV[1])

	// resubmit the query for the 2nd tile
	err = query.Submit()
	require.NoError(t, err)

	// verify query status
	status, err = query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// check that the second tile was returned
	assert.Equal(t, int32(6), bufferV[1])

	// verify status details
	details, err = query.StatusDetails()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_REASON_NONE, details.IncompleteReason)

	// close array
	err = array.Close()
	require.NoError(t, err)
}

// templateQueryPlan is the query plan expected for the arrays of the test.
// It is parameterized with the array URI which is different every time.
// The other fields should not change except there are changes in the array schema
// or the core query plan implementation.
const templateQueryPlan = `{
    "TileDB Query Plan": {
        "Array.Type": "dense",
        "Array.URI": "{{.uri}}",
        "Query.Attributes": [
            "v"
        ],
        "Query.Dimensions": [
            "x"
        ],
        "Query.Layout": "{{.layout}}",
        "Query.Strategy.Name": "{{.strategy}}",
        "VFS.Backend": "file"
    }
}`

// requirePlanAsExpected checks if a query plan conforms to the query plan template
func requirePlanAsExpected(t *testing.T, arrayPath, actualPlan string, diffs map[string]interface{}) {
	var expectedPlan bytes.Buffer
	require.NoError(t, template.Must(template.New("plan").Parse(templateQueryPlan)).Execute(&expectedPlan, diffs))

	m1 := map[string]any{}
	require.NoError(t, json.Unmarshal(expectedPlan.Bytes(), &m1))
	m2 := map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(actualPlan), &m2))

	require.True(t, reflect.DeepEqual(m1, m2))
}

func TestQueryPlan(t *testing.T) {
	// Create an 1d array

	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Create context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Create dimension
	dimension, err := NewDimension(context, "x", TILEDB_INT8, []int8{0, 9}, int8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Create domain
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
	attribute, err := NewAttribute(context, "v", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Add attribute to schema
	err = arraySchema.AddAttributes(attribute)
	require.NoError(t, err)

	// Set Domain
	err = arraySchema.SetDomain(domain)
	require.NoError(t, err)

	// Validate Schema
	err = arraySchema.Check()
	require.NoError(t, err)

	// Create array on disk
	tmpArrayPath := t.TempDir()
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)
	err = array.Create(arraySchema)
	require.NoError(t, err)

	// Write to array

	// Open array for writing
	err = array.Open(TILEDB_WRITE)
	require.NoError(t, err)

	// Create subarray
	subarray, err := array.NewSubarray()
	require.NoError(t, err)
	assert.NotNil(t, subarray)
	err = subarray.SetSubArray([]int8{0, 9})
	require.NoError(t, err)

	// Create write query
	query, err := NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	err = query.SetSubarray(subarray)
	require.NoError(t, err)

	// Initialize the data buffer
	bufferV := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	_, err = query.SetDataBuffer("v", bufferV)
	require.NoError(t, err)

	// Submit write query
	err = query.Submit()
	require.NoError(t, err)

	// Validate status, since query was used this is should be complete
	status, err := query.Status()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COMPLETED, status)

	// close array
	err = array.Close()
	require.NoError(t, err)

	// Read from the array. We will test an incomplete query
	// Open array for reading
	err = array.Open(TILEDB_READ)
	require.NoError(t, err)

	// Create subarray
	subarray, err = array.NewSubarray()
	require.NoError(t, err)
	assert.NotNil(t, subarray)
	err = subarray.SetSubArray([]int8{0, 9}) // we want to read the whole array, 2 full tiles
	require.NoError(t, err)

	// Create read query
	query, err = NewQuery(context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	err = query.SetSubarray(subarray)
	require.NoError(t, err)

	// Initialize the data buffer
	bufferV = make([]int32, 10)
	_, err = query.SetDataBuffer("v", bufferV)
	require.NoError(t, err)

	// Get query plan
	actualPlan, err := query.GetPlan()
	require.NoError(t, err)

	requirePlanAsExpected(t, tmpArrayPath, actualPlan, map[string]interface{}{
		"uri":      "file://" + tmpArrayPath,
		"layout":   "row-major",
		"strategy": "DenseReader",
	})
}
