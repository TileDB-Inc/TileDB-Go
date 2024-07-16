package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestArray(t *testing.T) *Array {
	// Create a 1d array

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

	return array
}

func writeTestArray(t *testing.T, array *Array, data []int32) {
	// Open array for writing
	err := array.Open(TILEDB_WRITE)
	require.NoError(t, err)

	// Create subarray
	subarray, err := array.NewSubarray()
	require.NoError(t, err)
	err = subarray.SetSubArray([]int8{0, 9})
	require.NoError(t, err)

	// Create write query
	query, err := NewQuery(array.context, array)
	require.NoError(t, err)
	assert.NotNil(t, query)
	err = query.SetSubarray(subarray)
	require.NoError(t, err)

	// Initialize the data buffer
	_, err = query.SetDataBuffer("v", data)
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
}

func TestGetConsolidationPlan(t *testing.T) {
	array := createTestArray(t)

	writeTestArray(t, array, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	checkConsolidationPlan := func(t *testing.T, cplan *ConsolidationPlan) {
		numNodes, err := cplan.NumNodes()
		require.NoError(t, err)
		assert.Equal(t, uint64(1), numNodes)

		for i := 0; i < int(numNodes); i++ {
			numFragments, err := cplan.NumFragments(uint64(i))
			require.NoError(t, err)
			assert.Equal(t, uint64(1), numFragments)
			fragmentURI, err := cplan.FragmentURI(uint64(i), uint64(0))
			require.NoError(t, err)

			// fragment uris in the plan are relative
			fullPath := filepath.Join(array.uri, "__fragments", fragmentURI)
			_, err = os.Stat(fullPath)
			require.NoError(t, err)
		}
	}

	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)
	arr, err := NewArray(tdbCtx, array.uri)
	require.NoError(t, err)
	require.NoError(t, arr.Open(TILEDB_READ))
	t.Cleanup(func() { arr.Close() })

	cplan, err := GetConsolidationPlan(arr, 1)
	require.NoError(t, err)

	checkConsolidationPlan(t, cplan)
}

func TestConsolidateFragments(t *testing.T) {
	// The test is skipped pending a core release for 2.25.0 that includes this fix:
	// https://github.com/TileDB-Inc/TileDB/pull/5135
	t.Skip("Skipping fragment list consolidation SC-51140")

	array := createTestArray(t)

	numFrags := uint32(5)
	for i := uint32(0); i < numFrags; i++ {
		writeTestArray(t, array, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	}

	fragmentInfo, err := NewFragmentInfo(array.context, array.uri)
	require.NoError(t, err)

	err = fragmentInfo.Load()
	require.NoError(t, err)

	fragInfoNum, err := fragmentInfo.GetFragmentNum()
	require.NoError(t, err)
	require.Equal(t, numFrags, fragInfoNum)
	fragUris := make([]string, numFrags)
	for i := uint32(0); i < numFrags; i++ {
		uri, err := fragmentInfo.GetFragmentURI(i)
		require.NoError(t, err)
		fragUris[i] = uri
	}

	// Default consolidation mode is 'fragments'.
	config, err := array.context.Config()
	require.NoError(t, err)

	err = array.ConsolidateFragments(config, fragUris)
	require.NoError(t, err)

	// Check that the new consolidated fragment was created.
	err = fragmentInfo.Load()
	require.NoError(t, err)
	fragInfoNum, err = fragmentInfo.GetFragmentNum()
	require.NoError(t, err)
	fragToVacuumNum, err := fragmentInfo.GetToVacuumNum()
	require.NoError(t, err)
	require.Equal(t, numFrags, fragToVacuumNum)
	require.Equal(t, uint32(1), fragInfoNum)

	err = array.Vacuum(config)
	require.NoError(t, err)

	// Check for one fragment after vacuum.
	err = fragmentInfo.Load()
	require.NoError(t, err)
	fragInfoNum, err = fragmentInfo.GetFragmentNum()
	require.NoError(t, err)
	fragToVacuumNum, err = fragmentInfo.GetToVacuumNum()
	require.NoError(t, err)
	require.Equal(t, uint32(1), fragInfoNum)
	require.Equal(t, uint32(0), fragToVacuumNum)
}
