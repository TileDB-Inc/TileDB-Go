package tiledb

import (
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func create1DTestArray(t *testing.T) *Array {
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
	err = CreateArray(context, tmpArrayPath, arraySchema)
	require.NoError(t, err)

	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)
	return array
}

func write1DTestArray(t *testing.T, array *Array, data []int32) {
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
	array := create1DTestArray(t)

	write1DTestArray(t, array, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

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
	for _, test := range []struct {
		name         string
		relativeURIs bool
	}{
		{"fragment list consolidation with relative URIs", true},
		{"fragment list consolidation with absolute URIs", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			array := create1DTestArray(t)

			numFrags := 5
			for i := 0; i < numFrags; i++ {
				write1DTestArray(t, array, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
			}

			fragmentInfo, err := NewFragmentInfo(array.context, array.uri)
			require.NoError(t, err)

			err = fragmentInfo.Load()
			require.NoError(t, err)

			fragInfoNum, err := fragmentInfo.GetFragmentNum()
			require.NoError(t, err)
			require.EqualValues(t, numFrags, fragInfoNum)
			fragUris := make([]string, numFrags)
			for i := 0; i < numFrags; i++ {
				uri, err := fragmentInfo.GetFragmentURI(uint32(i))
				require.NoError(t, err)
				fragUris[i] = uri
				if test.relativeURIs {
					fragUris[i] = path.Base(fragUris[i])
				}
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
			require.EqualValues(t, numFrags, fragToVacuumNum)
			require.Equal(t, uint32(1), fragInfoNum)

			err = VacuumArray(array.context, array.uri, config)
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
		})
	}
}
