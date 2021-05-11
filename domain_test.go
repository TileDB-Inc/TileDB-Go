package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	hasDim, err := domain.HasDimension("test")
	assert.Nil(t, err)
	assert.Equal(t, false, hasDim)

	// Add dimension
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	hasDim, err = domain.HasDimension("test")
	assert.Nil(t, err)
	assert.Equal(t, true, hasDim)

	// Test getting type
	datatype, err := domain.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Test getting number of dimension
	ndim, err := domain.NDim()
	assert.Nil(t, err)
	assert.Equal(t, uint(1), ndim)

	// Test getting dimension from index for domain
	dimensionFromIndex, err := domain.DimensionFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Validate dimension returned
	dimensionName, err := dimensionFromIndex.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", dimensionName)

	// Test getting dimension from name for domain
	dimensionFromName, err := domain.DimensionFromName(dimensionName)
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Validate dimension returned
	dimensionName, err = dimensionFromName.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", dimensionName)

	// Temp path for testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_domain_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	err = domain.Dump(tmpPathDump)
	assert.Nil(t, err)
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	err = domain.DumpSTDOUT()
	assert.Nil(t, err)

	domain.Free()
}
