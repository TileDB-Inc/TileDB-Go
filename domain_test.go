package tiledb

import (
	"github.com/stretchr/testify/assert"
	"testing"
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
	err = domain.AddDimension(*dimension)
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
	dimension, err := NewDimension(context, "test", []int32{1, 10}, 5)
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimension(*dimension)
	assert.Nil(t, err)

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

	domain.Free()
}
