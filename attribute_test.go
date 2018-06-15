package tiledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func ExampleNewAttribute() {
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

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	if err != nil {
		// Handle error
		return
	}

	// Set compressor
	attribute.SetCompressor(Compressor{Compressor: TILEDB_GZIP, Level: -1})

	// Set Cell Value Number
	err = attribute.SetCellValNum(10)
	if err != nil {
		// Handle error
		return
	}
}

//TestNewAttribute tests setting a new context
func TestNewAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	attribute.Free()
}

func ExampleAttribute_SetCompressor() {
	// Create configuration
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

	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	if err != nil {
		// Handle error
		return
	}
	attribute.SetCompressor(Compressor{Compressor: TILEDB_GZIP, Level: -1})
	compressor, err := attribute.Compressor()
	if err != nil {
		// Handle error
		return
	}
	// Output: GZIP
	fmt.Println(compressor.Str())

}

func TestFullAttribute(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Create Attribute
	attribute, err := NewAttribute(context, "test", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Get Attribute Name
	name, err := attribute.Name()
	assert.Nil(t, err)
	assert.Equal(t, "test", name)

	// Get Attribute Datatype
	datatype, err := attribute.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	// Get and set compressor
	compressor, err := attribute.Compressor()
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	assert.Equal(t, Compressor{Compressor: TILEDB_NO_COMPRESSION, Level: -1}, *compressor)

	attribute.SetCompressor(Compressor{Compressor: TILEDB_GZIP, Level: -1})
	compressor, err = attribute.Compressor()
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	assert.Equal(t, Compressor{Compressor: TILEDB_GZIP, Level: -1}, *compressor)

	// Set Cell Value Number
	err = attribute.SetCellValNum(10)
	assert.Nil(t, err)

	cellValNum, err := attribute.CellValNum()
	assert.Nil(t, err)
	assert.Equal(t, uint(10), cellValNum)
}
