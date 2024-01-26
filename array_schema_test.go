package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
}

// TestArraySchema tests creating a new dimension
func TestArraySchema(t *testing.T) {
	// Create configuration
	config, err := NewConfig()
	require.NoError(t, err)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", TILEDB_INT32, []int32{1, 10}, int32(5))
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

	// Dense array, allowDups should be false
	allowDups, err := arraySchema.AllowsDups()
	require.NoError(t, err)
	assert.Equal(t, false, allowDups)

	// Create attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	require.NoError(t, arraySchema.AddAttributes(attribute))

	attrNum, err := arraySchema.AttributeNum()
	require.NoError(t, err)
	assert.Equal(t, uint(1), attrNum)

	attrFromIndex, err := arraySchema.AttributeFromIndex(0)
	require.NoError(t, err)
	assert.NotNil(t, attrFromIndex)

	attrName, err := attrFromIndex.Name()
	require.NoError(t, err)
	assert.Equal(t, "a1", attrName)

	attrFromName, err := arraySchema.AttributeFromName(attrName)
	require.NoError(t, err)
	assert.NotNil(t, attrFromName)

	attrName2, err := attrFromName.Name()
	require.NoError(t, err)
	assert.Equal(t, "a1", attrName2)

	hasAttr, err := arraySchema.HasAttribute("a1")
	require.NoError(t, err)
	assert.Equal(t, true, hasAttr)

	hasAttr, err = arraySchema.HasAttribute("a2")
	require.NoError(t, err)
	assert.Equal(t, false, hasAttr)

	// Set Capacity
	require.NoError(t, arraySchema.SetCapacity(100))

	// Get Capacity
	capacity, err := arraySchema.Capacity()
	require.NoError(t, err)
	assert.Equal(t, uint64(100), capacity)

	require.NoError(t, arraySchema.SetDomain(domain))

	// Test getting domain
	domain, err = arraySchema.Domain()
	require.NoError(t, err)
	assert.NotNil(t, domain)
	assert.NotNil(t, domain.tiledbDomain)

	// Validate returned domains have equal type and number
	domainNdim, err := domain.NDim()
	require.NoError(t, err)
	assert.NotZero(t, domainNdim)

	domainDatatype, err := domain.Type()
	require.NoError(t, err)
	assert.True(t, domainDatatype > -1)

	// Set Cell Order
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_GLOBAL_ORDER))

	cellOrder, err := arraySchema.CellOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_GLOBAL_ORDER, cellOrder)

	// Set Tile Order
	require.NoError(t, arraySchema.SetTileOrder(TILEDB_COL_MAJOR))

	tileOrder, err := arraySchema.TileOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_COL_MAJOR, tileOrder)

	// Set Coordinates Filter List
	filter, err := NewFilter(context, TILEDB_FILTER_BZIP2)
	require.NoError(t, err)
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, arraySchema.SetCoordsFilterList(filterList))

	filterListReturn, err := arraySchema.CoordsFilterList()
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err := filterListReturn.FilterFromIndex(0)
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err := filterReturn.Type()
	require.NoError(t, err)
	assert.EqualValues(t, TILEDB_FILTER_BZIP2, filterTypeReturn)

	// Set Offsets Compressor
	require.NoError(t, arraySchema.SetOffsetsFilterList(filterList))

	filterListReturn, err = arraySchema.OffsetsFilterList()
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err = filterListReturn.FilterFromIndex(0)
	require.NoError(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err = filterReturn.Type()
	require.NoError(t, err)
	assert.EqualValues(t, TILEDB_FILTER_BZIP2, filterTypeReturn)

	require.NoError(t, arraySchema.Check())

	// Temp path for testing dump
	tmpPathDump := filepath.Join(t.TempDir(), "dumpfile")

	schemaType, err := arraySchema.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_DENSE, schemaType)

	// Test dumping to file
	require.NoError(t, arraySchema.Dump(tmpPathDump))
	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPathDump)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	require.NoError(t, arraySchema.DumpSTDOUT())

	arraySchema.Free()
}

func TestArraySchemaInt32Hilbert(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{0, 100}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{0, 200}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(d1, d2))
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, a)
	require.NoError(t, arraySchema.AddAttributes(a))
	require.NoError(t, arraySchema.SetDomain(domain))
	// Set Cell Order
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.SetCapacity(2))
	require.NoError(t, arraySchema.Check())
	arraySchema.Free()
}

func TestArraySchemaInt32DenseHilbert(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{0, 100}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{0, 200}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(d1, d2))
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, a)
	require.NoError(t, arraySchema.AddAttributes(a))
	require.NoError(t, arraySchema.SetDomain(domain))
	// Set Cell Order
	// Hilbert not applicable to dense
	assert.Error(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	// Hilbert order only applicable to cells
	assert.Error(t, arraySchema.SetTileOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.SetCapacity(2))
	require.NoError(t, arraySchema.Check())
	arraySchema.Free()
}

func TestArraySchemaInt32NegativeDomainHilbert(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{-50, 50}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{-100, 100}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(d1, d2))
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, a)
	require.NoError(t, arraySchema.AddAttributes(a))
	require.NoError(t, arraySchema.SetDomain(domain))
	// Set Cell Order
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.SetCapacity(2))
	require.NoError(t, arraySchema.Check())
	arraySchema.Free()
}

func TestArraySchemaFloat32Hilbert(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_FLOAT32, []float32{0.0, 1.0}, float32(0.01))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_FLOAT32, []float32{0.0, 2.0}, float32(0.01))
	require.NoError(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(d1, d2))
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, a)
	require.NoError(t, arraySchema.AddAttributes(a))
	require.NoError(t, arraySchema.SetDomain(domain))
	// Set Cell Order
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.SetCapacity(2))
	require.NoError(t, arraySchema.Check())
	arraySchema.Free()
}

func TestArraySchemaStringHilbert(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	context, err := NewContext(config)
	require.NoError(t, err)
	d1, err := NewStringDimension(context, "d1")
	require.NoError(t, err)
	assert.NotNil(t, d1)
	d2, err := NewStringDimension(context, "d2")
	require.NoError(t, err)
	assert.NotNil(t, d2)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	require.NoError(t, domain.AddDimensions(d1, d2))
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	require.NoError(t, err)
	assert.NotNil(t, a)
	require.NoError(t, arraySchema.AddAttributes(a))
	require.NoError(t, arraySchema.SetDomain(domain))
	// Set Cell Order
	require.NoError(t, arraySchema.SetCellOrder(TILEDB_HILBERT))
	require.NoError(t, arraySchema.SetCapacity(2))
	require.NoError(t, arraySchema.Check())
	arraySchema.Free()
}
