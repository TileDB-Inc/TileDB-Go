//go:build experimental

package tiledb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleDimensionLabel_Free() {
	context, err := NewContext(nil)
	if err != nil {
		return
	}
	defer context.Free()

	// Create Dimension, Domain, Attribute, ArraySchema
	dim, err := NewDimension(context, "d1", TILEDB_INT32, []int32{1, 10}, int32(5))
	if err != nil {
		return
	}
	defer dim.Free()
	domain, err := NewDomain(context)
	if err != nil {
		return
	}
	defer domain.Free()
	schema, err := NewArraySchema(context, TILEDB_SPARSE)
	if err != nil {
		return
	}
	defer schema.Free()
	err = domain.AddDimensions(dim)
	if err != nil {
		return
	}
	attr, err := NewAttribute(context, "a1", TILEDB_INT32)
	if err != nil {
		return
	}
	defer attr.Free()
	err = schema.AddAttributes(attr)
	if err != nil {
		return
	}
	err = schema.SetDomain(domain)
	if err != nil {
		return
	}

	// Add dimension label to schema.
	err = schema.AddDimensionLabel(0, "label_name", TILEDB_INCREASING_DATA, TILEDB_FLOAT64)
	if err != nil {
		return
	}

	array, err := NewArray(context, "dimlabel_example")
	if err != nil {
		return
	}
	defer array.Free()
	objectType, err := ObjectType(context, "dimlabel_example")
	if err != nil {
		return
	}
	if objectType == TILEDB_ARRAY {
		err = os.RemoveAll("dimlabel_example")
		if err != nil {
			return
		}
	}
	err = array.Create(schema)
	if err != nil {
		return
	}
	err = array.Open(TILEDB_WRITE)
	defer func(array *Array) {
		err := array.Close()
		if err != nil {
			return
		}
	}(array)

	query, err := NewQuery(context, array)
	if err != nil {
		return
	}
	defer query.Free()

	writeDim := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	writeAttr := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	writeLabel := []float64{-1.0, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8}
	_, err = query.SetDataBuffer("d1", writeDim)
	if err != nil {
		return
	}
	_, err = query.SetDataBuffer("a1", writeAttr)
	if err != nil {
		return
	}
	_, err = query.SetDataBuffer("label_name", writeLabel)
	if err != nil {
		return
	}
	err = query.Submit()
	if err != nil {
		return
	}

	err = query.Finalize()
	if err != nil {
		return
	}

	// Read dimension label data.
	err = array.Close()
	if err != nil {
		return
	}
	err = array.Open(TILEDB_READ)
	if err != nil {
		return
	}

	// ToDo: Add test using label ranges when tiledb_subarray_add_label_range is implemented.
	query.Free()
	query, err = NewQuery(context, array)
	defer query.Free()

	err = query.AddRange(0, 1, 10)
	if err != nil {
		return
	}
	readLabel := make([]float64, 10)
	_, err = query.SetDataBuffer("label_name", readLabel)
	if err != nil {
		return
	}
	err = query.Submit()
	if err != nil {
		return
	}

	// Verify output matches expected dim label data.
	fmt.Println(readLabel)
	// Output: [-1 -0.8 -0.6 -0.4 -0.2 0 0.2 0.4 0.6 0.8]
}

// TestDimension Tests created dimension labels have expected schema values.
func TestDimensionLabelSchema(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)

	// Create dimensions.
	dimension, err := NewDimension(context, "d0", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	dimension2, err := NewDimension(context, "d1", TILEDB_INT32, []int32{1.0, 10.0}, int32(5.0))
	require.NoError(t, err)
	assert.NotNil(t, dimension2)
	// Create schema and domain.
	schema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, schema)
	domain, err := NewDomain(context)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(dimension, dimension2))
	require.NoError(t, schema.SetDomain(domain))

	// Unordered dimension labels are not yet supported.
	require.Error(t, schema.AddDimensionLabel(0, "d0_label0", TILEDB_UNORDERED_DATA, TILEDB_FLOAT64))

	require.NoError(t, schema.AddDimensionLabel(0, "d0_label0", TILEDB_INCREASING_DATA, TILEDB_FLOAT64))
	require.NoError(t, schema.AddDimensionLabel(0, "d0_label1", TILEDB_DECREASING_DATA, TILEDB_INT64))
	require.NoError(t, schema.AddDimensionLabel(1, "d1_label0", TILEDB_DECREASING_DATA, TILEDB_FLOAT32))

	dimLabelCheck(t, schema, "d0_label0", "__labels/l0", TILEDB_FLOAT64, TILEDB_INCREASING_DATA, 1)
	dimLabelCheck(t, schema, "d0_label1", "__labels/l1", TILEDB_INT64, TILEDB_DECREASING_DATA, 1)
	dimLabelCheck(t, schema, "d1_label0", "__labels/l2", TILEDB_FLOAT32, TILEDB_DECREASING_DATA, 1)

	err = schema.SetDimensionLabelTileExtent("d0_label0", TILEDB_INT32, int32(2))
	require.NoError(t, err)

	// Write the array and schemas to disk to validate dimension label extent.
	array, err := NewArray(context, "dimlabel_schema_test")
	if err != nil {
		return
	}
	defer array.Free()
	objectType, err := ObjectType(context, "dimlabel_schema_test")
	if err != nil {
		return
	}
	if objectType == TILEDB_ARRAY {
		err = os.RemoveAll("dimlabel_schema_test")
		if err != nil {
			return
		}
	}
	err = array.Create(schema)

	labelSchema, err := LoadArraySchema(context, "dimlabel_schema_test/__labels/l0")
	require.NoError(t, err)
	require.NotNil(t, labelSchema)
	labelDomain, err := labelSchema.Domain()
	require.NoError(t, err)
	require.NotNil(t, labelDomain)
	labelDim, err := labelDomain.DimensionFromIndex(0)
	require.NoError(t, err)
	require.NotNil(t, labelDim)
	labelExtent, err := labelDim.Extent()
	require.NoError(t, err)
	require.NotNil(t, labelExtent)
	require.Equal(t, labelExtent, int32(2))

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, schema.SetDimensionLabelFilterList("d1_label0", *filterList))
}

// dimLabelCheck Retrieve a dimension label from schema by name and check expected values.
func dimLabelCheck(t *testing.T, schema *ArraySchema, name string, uri string, labelType Datatype, labelOrder DataOrder, cellValNum uint32) {
	dimLabel, err := schema.DimensionLabelFromName(name)
	dimLabelType, err := dimLabel.Type()
	require.NoError(t, err)
	assert.Equal(t, labelType, dimLabelType)
	dimLabelOrder, err := dimLabel.LabelOrder()
	require.NoError(t, err)
	assert.Equal(t, labelOrder, dimLabelOrder)
	dimLabelName, err := dimLabel.Name()
	require.NoError(t, err)
	assert.Equal(t, name, dimLabelName)
	dimLabelUri, err := dimLabel.Uri()
	require.NoError(t, err)
	assert.Equal(t, uri, dimLabelUri)
	dimLabelAttrName, err := dimLabel.LabelAttrName()
	require.NoError(t, err)
	assert.Equal(t, "label", dimLabelAttrName)
	dimLabelCellValNum, err := dimLabel.LabelCellValNum()
	require.NoError(t, err)
	assert.Equal(t, cellValNum, dimLabelCellValNum)
	dimLabel.Free()
}
