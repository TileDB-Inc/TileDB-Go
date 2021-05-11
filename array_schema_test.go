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

	// Crete attribute to add to schema
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
	assert.Nil(t, err)

	// Test context with config
	context, err := NewContext(config)
	assert.Nil(t, err)

	// Test create dimension
	dimension, err := NewDimension(context, "dim1", TILEDB_INT32, []int32{1, 10}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, dimension)

	// Test creating domain
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)

	// Add dimension
	err = domain.AddDimensions(dimension)
	assert.Nil(t, err)

	// Create array schema
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)

	// Dense array, allowDups should be false
	allowDups, err := arraySchema.AllowsDups()
	assert.Nil(t, err)
	assert.Equal(t, false, allowDups)

	// Crete attribute to add to schema
	attribute, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, attribute)

	// Add Attribute
	err = arraySchema.AddAttributes(attribute)
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

	hasAttr, err := arraySchema.HasAttribute("a1")
	assert.Nil(t, err)
	assert.Equal(t, true, hasAttr)

	hasAttr, err = arraySchema.HasAttribute("a2")
	assert.Nil(t, err)
	assert.Equal(t, false, hasAttr)

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

	// Set Coordinates Filter List
	filter, err := NewFilter(context, TILEDB_FILTER_BZIP2)
	assert.Nil(t, err)
	filterList, err := NewFilterList(context)
	assert.Nil(t, err)
	err = filterList.AddFilter(filter)
	assert.Nil(t, err)
	err = arraySchema.SetCoordsFilterList(filterList)
	assert.Nil(t, err)

	filterListReturn, err := arraySchema.CoordsFilterList()
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err := filterListReturn.FilterFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err := filterReturn.Type()
	assert.Nil(t, err)
	assert.EqualValues(t, TILEDB_FILTER_BZIP2, filterTypeReturn)

	// Set Offsets Compressor
	err = arraySchema.SetOffsetsFilterList(filterList)
	assert.Nil(t, err)

	filterListReturn, err = arraySchema.OffsetsFilterList()
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterReturn, err = filterListReturn.FilterFromIndex(0)
	assert.Nil(t, err)
	assert.NotNil(t, filterListReturn)
	filterTypeReturn, err = filterReturn.Type()
	assert.Nil(t, err)
	assert.EqualValues(t, TILEDB_FILTER_BZIP2, filterTypeReturn)

	err = arraySchema.Check()
	assert.Nil(t, err)

	// Temp path froo+= testing dump
	tmpPathDump := os.TempDir() + string(os.PathSeparator) + "tiledb_array_schema_dump_test"
	// Cleanup tmp file when test ends
	defer os.RemoveAll(tmpPathDump)
	if _, err = os.Stat(tmpPathDump); err == nil {
		os.RemoveAll(tmpPathDump)
	}

	schemaType, err := arraySchema.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_DENSE, schemaType)

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

func TestArraySchemaInt32Hilbert(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	context, err := NewContext(config)
	assert.Nil(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{0, 100}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{0, 200}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	err = domain.AddDimensions(d1, d2)
	assert.Nil(t, err)
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a)
	err = arraySchema.AddAttributes(a)
	assert.Nil(t, err)
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)
	// Set Cell Order
	err = arraySchema.SetCellOrder(TILEDB_HILBERT)
	assert.Nil(t, err)
	err = arraySchema.SetCapacity(2)
	assert.Nil(t, err)
	err = arraySchema.Check()
	assert.Nil(t, err)
	arraySchema.Free()
}

func TestArraySchemaInt32DenseHilbert(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	context, err := NewContext(config)
	assert.Nil(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{0, 100}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{0, 200}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	err = domain.AddDimensions(d1, d2)
	assert.Nil(t, err)
	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a)
	err = arraySchema.AddAttributes(a)
	assert.Nil(t, err)
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)
	// Set Cell Order
	// Hilbert not applicable to dense
	err = arraySchema.SetCellOrder(TILEDB_HILBERT)
	assert.NotNil(t, err)
	// Hilbert order only applicable to cells
	err = arraySchema.SetTileOrder(TILEDB_HILBERT)
	assert.NotNil(t, err)
	err = arraySchema.SetCapacity(2)
	assert.Nil(t, err)
	err = arraySchema.Check()
	assert.Nil(t, err)
	arraySchema.Free()
}

func TestArraySchemaInt32NegativeDomainHilbert(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	context, err := NewContext(config)
	assert.Nil(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_INT32, []int32{-50, 50}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_INT32, []int32{-100, 100}, int32(5))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	err = domain.AddDimensions(d1, d2)
	assert.Nil(t, err)
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a)
	err = arraySchema.AddAttributes(a)
	assert.Nil(t, err)
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)
	// Set Cell Order
	err = arraySchema.SetCellOrder(TILEDB_HILBERT)
	assert.Nil(t, err)
	err = arraySchema.SetCapacity(2)
	assert.Nil(t, err)
	err = arraySchema.Check()
	assert.Nil(t, err)
	arraySchema.Free()
}

func TestArraySchemaFloat32Hilbert(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	context, err := NewContext(config)
	assert.Nil(t, err)
	d1, err := NewDimension(context, "d1", TILEDB_FLOAT32, []float32{0.0, 1.0}, float32(0.01))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	d2, err := NewDimension(context, "d2", TILEDB_FLOAT32, []float32{0.0, 2.0}, float32(0.01))
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	err = domain.AddDimensions(d1, d2)
	assert.Nil(t, err)
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a)
	err = arraySchema.AddAttributes(a)
	assert.Nil(t, err)
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)
	// Set Cell Order
	err = arraySchema.SetCellOrder(TILEDB_HILBERT)
	assert.Nil(t, err)
	err = arraySchema.SetCapacity(2)
	assert.Nil(t, err)
	err = arraySchema.Check()
	assert.Nil(t, err)
	arraySchema.Free()
}

func TestArraySchemaStringHilbert(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	context, err := NewContext(config)
	assert.Nil(t, err)
	d1, err := NewStringDimension(context, "d1")
	assert.Nil(t, err)
	assert.NotNil(t, d1)
	d2, err := NewStringDimension(context, "d2")
	assert.Nil(t, err)
	assert.NotNil(t, d2)
	domain, err := NewDomain(context)
	assert.Nil(t, err)
	assert.NotNil(t, domain)
	err = domain.AddDimensions(d1, d2)
	assert.Nil(t, err)
	arraySchema, err := NewArraySchema(context, TILEDB_SPARSE)
	assert.Nil(t, err)
	assert.NotNil(t, arraySchema)
	a, err := NewAttribute(context, "a1", TILEDB_INT32)
	assert.Nil(t, err)
	assert.NotNil(t, a)
	err = arraySchema.AddAttributes(a)
	assert.Nil(t, err)
	err = arraySchema.SetDomain(domain)
	assert.Nil(t, err)
	// Set Cell Order
	err = arraySchema.SetCellOrder(TILEDB_HILBERT)
	assert.Nil(t, err)
	err = arraySchema.SetCapacity(2)
	assert.Nil(t, err)
	err = arraySchema.Check()
	assert.Nil(t, err)
	arraySchema.Free()
}
