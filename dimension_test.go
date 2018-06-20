package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func ExampleNewDimension() {
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
	_, err = NewDimension(context, "test", []int32{1, 10}, int32(5))
	if err != nil {
		// Handle error
		return
	}

}

// TestDimension tests creating a new dimension
func TestDimension(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Dimension will error due to extent and domain having different datatypes
	dimension, err := NewDimension(context, "test", []int32{1, 10}, 5)
	assert.NotNil(t, err)
	assert.Nil(t, dimension)

	// Create dimension
	dimension, err = NewDimension(context, "test", []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	name, err := dimension.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", name)

	datatype, err := dimension.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	dimension.Free()
}

// TestDimensionDomainTypes tests creating dimension of all domain types
func TestDimensionDomainTypes(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	dimension, err := NewDimension(context, "test", []int{1, 10}, int(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", []int8{1, 10}, int8(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	domain, err := dimension.Domain()
	assert.Nil(t, err)
	// Test getting domain
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int8{1, 10}, domain)
	extent, err := dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int8(5), extent)

	dimension, err = NewDimension(context, "test", []int16{1, 10}, int16(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int16{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int16(5), extent)

	dimension, err = NewDimension(context, "test", []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int32{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int32(5), extent)

	dimension, err = NewDimension(context, "test", []int64{1, 10}, int64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	// Test getting extent
	assert.EqualValues(t, []int64{1, 10}, domain)
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, int64(5), extent)

	dimension, err = NewDimension(context, "test", []uint{1, 10}, uint(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	dimension, err = NewDimension(context, "test", []uint8{1, 10}, uint8(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint8{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint8(5), extent)

	dimension, err = NewDimension(context, "test", []uint16{1, 10}, uint16(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint16{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint16(5), extent)

	dimension, err = NewDimension(context, "test", []uint32{1, 10}, uint32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint32(5), extent)

	dimension, err = NewDimension(context, "test", []uint64{1, 10}, uint64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []uint64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, uint64(5), extent)

	dimension, err = NewDimension(context, "test", []float32{1, 10}, float32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float32{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float32(5), extent)

	dimension, err = NewDimension(context, "test", []float64{1, 10}, float64(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)
	// Test getting domain
	domain, err = dimension.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.EqualValues(t, []float64{1, 10}, domain)
	// Test getting extent
	extent, err = dimension.Extent()
	assert.Nil(t, err)
	assert.NotNil(t, extent)
	assert.EqualValues(t, float64(5), extent)
}
