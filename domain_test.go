package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleNewDomain() {
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
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
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
}

// TestDomain tests creating a new dimension
func TestDomain(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)

	hasDim, err := domain.HasDimension("test")
	require.NoError(t, err)
	assert.Equal(t, false, hasDim)

	// Add dimension
	require.NoError(t, domain.AddDimensions(dimension))

	hasDim, err = domain.HasDimension("test")
	require.NoError(t, err)
	assert.Equal(t, true, hasDim)

	// Test getting type
	datatype, err := domain.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Test getting number of dimension
	ndim, err := domain.NDim()
	require.NoError(t, err)
	assert.Equal(t, uint(1), ndim)

	// Test getting dimension from index for domain
	dimensionFromIndex, err := domain.DimensionFromIndex(0)
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Validate dimension returned
	dimensionName, err := dimensionFromIndex.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", dimensionName)

	// Test getting dimension from name for domain
	dimensionFromName, err := domain.DimensionFromName(dimensionName)
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	// Validate dimension returned
	dimensionName, err = dimensionFromName.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", dimensionName)

	// Temp path for testing dump
	tmpPathDump := filepath.Join(t.TempDir(), "dump")

	// Test dumping to file
	require.NoError(t, domain.Dump(tmpPathDump))
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	require.NoError(t, domain.DumpSTDOUT())

	domain.Free()
}
