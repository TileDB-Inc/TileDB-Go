//go:build experimental
// +build experimental

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

// TestDimension tests creating a new dimension label
func TestDimensionLabel(t *testing.T) {
	context, err := NewContext(nil)
	require.NoError(t, err)

	// Create dimension
	dimension, err := NewDimension(context, "test", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)

	dimension2, err := NewDimension(context, "test2", TILEDB_INT32, []int32{1.0, 10.0}, int32(5.0))
	require.NoError(t, err)
	assert.NotNil(t, dimension2)

	name, err := dimension.Name()
	require.NoError(t, err)
	assert.Equal(t, "test", name)

	datatype, err := dimension.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INT32, datatype)

	schema, err := NewArraySchema(context, TILEDB_SPARSE)
	require.NoError(t, err)
	assert.NotNil(t, schema)

	domain, err := NewDomain(context)
	require.NoError(t, err)

	require.NoError(t, domain.AddDimensions(dimension, dimension2))
	require.NoError(t, schema.SetDomain(domain))
	// Unordered dimension labels are not yet supported.
	require.Error(t, schema.AddDimensionLabel(0, "test_label", TILEDB_UNORDERED_DATA, TILEDB_FLOAT64))
	require.NoError(t, schema.AddDimensionLabel(0, "test_label", TILEDB_INCREASING_DATA, TILEDB_FLOAT64))

	dimLabelNum, err := schema.DimensionLabelNum()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), dimLabelNum)

	require.NoError(t, schema.AddDimensionLabel(1, "test_label2", TILEDB_DECREASING_DATA, TILEDB_FLOAT64))

	dimLabelNum, err = schema.DimensionLabelNum()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), dimLabelNum)

	dimLabel, err := schema.DimensionLabelFromIndex(0)
	require.NoError(t, err)
	require.NotNil(t, dimLabel)

	dimIndex, err := dimLabel.DimensionIndex()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), dimIndex)

	dimLabelFromName, err := schema.DimensionLabelFromName("test_label2")
	require.NoError(t, err)
	require.NotNil(t, dimLabelFromName)
	assert.Equal(t, dimLabel, dimLabelFromName)

	dimLabelOrder, err := dimLabel.LabelOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_INCREASING_DATA, dimLabelOrder)

	dimLabelOrder, err = dimLabelFromName.LabelOrder()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_DECREASING_DATA, dimLabelOrder)

	dimLabelName, err := dimLabel.Name()
	assert.Equal(t, "test_label", dimLabelName)

	datatype, err = dimLabel.Type()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_FLOAT64, datatype)

	dimLabelUri, err := dimLabel.Uri()
	require.NoError(t, err)
	assert.Equal(t, "__labels/l0", dimLabelUri)

	dimLabelAttrName, err := dimLabel.LabelAttrName()
	require.NoError(t, err)
	assert.Equal(t, "label", dimLabelAttrName)

	dimLabelCellValNum, err := dimLabel.LabelCellValNum()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), dimLabelCellValNum)

	// Get and set compressor
	filter, err := NewFilter(context, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(context)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, schema.SetDimensionLabelFilterList("test_label", *filterList))

	dimLabel.Free()
}
