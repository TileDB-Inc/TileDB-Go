package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func ExampleNewArraySchema() {
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

	err = arraySchema.AddAttributes(*attribute)
	if err != nil {
		// Handle error
		return
	}
}

// TestArraySchema tests creating a new dimension
func TestArraySchema(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimension(*dimension)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	err = arraySchema.AddAttributes(*attribute)
	assert.Nil(t, err)

	attrNum, err := arraySchema.AttributeNum()
	assert.Nil(t, err)
	assert.Equal(t, uint(1), attrNum)

	attrFromIndex, err := arraySchema.AttributeFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, attrFromIndex)

	attrName, err := attrFromIndex.Name()
	assert.Nil(t, err)
	assert.Equal(t, "a1", attrName)

	attrFromName, err := arraySchema.AttributeFromName(attrName)
	assert.Nil(t, err)
	assert.NotNil(t, attrFromName)

	attrName2, err := attrFromName.Name()
	assert.Nil(t, err)
	assert.Equal(t, "a1", attrName2)

	// Set Capacity
	err = arraySchema.SetCapacity(100)
	assert.Nil(t, err)

	// Get Capacity
	capacity, err := arraySchema.Capacity()
	assert.Nil(t, err)
	assert.Equal(t, uint64(100), capacity)

	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)

	// Test getting domain
	domain, err = arraySchema.Domain()
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	assert.NotNil(t, domain.tiledbDomain)

	// Validate returned domains have equal type and number
	domainNdim, err := domain.NDim()
	assert.Nil(t, err)
	assert.NotZero(t, domainNdim)

	domainDatatype, err := domain.Type()
	assert.Nil(t, err)
	assert.True(t, domainDatatype > -1)

	// Set Cell Order
	err = arraySchema.SetCellOrder(TILEDB_GLOBAL_ORDER)
	assert.Nil(t, err)

	cellOrder, err := arraySchema.CellOrder()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_GLOBAL_ORDER, cellOrder)

	// Set Tile Order
	err = arraySchema.SetTileOrder(TILEDB_COL_MAJOR)
	assert.Nil(t, err)

	tileOrder, err := arraySchema.TileOrder()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_COL_MAJOR, tileOrder)

	// Set Coordinates Compressor
	c := Compressor{Compressor: TILEDB_BZIP2, Level: -1}
	err = arraySchema.SetCoordsCompressor(c)
	assert.Nil(t, err)

	compressor, err := arraySchema.CoordsCompressor()
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	assert.EqualValues(t, c, *compressor)

	// Set Offsets Compressor
	err = arraySchema.SetOffsetsCompressor(c)
	assert.Nil(t, err)

	compressor, err = arraySchema.OffsetsCompressor()
	assert.Nil(t, err)
	assert.NotNil(t, compressor)
	assert.EqualValues(t, c, *compressor)

	err = arraySchema.Check()
	assert.Nil(t, err)

	// Temp path froo+= testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_array_schema_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	// Test dumping to file
	err = arraySchema.Dump(tmpPathDump)
	assert.Nil(t, err)
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	err = arraySchema.DumpSTDOUT()
	assert.Nil(t, err)

	arraySchema.Free()
}
