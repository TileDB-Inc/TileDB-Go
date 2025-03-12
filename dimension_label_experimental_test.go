package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDimensionLabelQuery(t *testing.T) {
	schema := schemaSparseWithDimensionLabels(t)

	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	// create the array
	uri := t.TempDir()
	array, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Create(schema))

	// initialize the labels
	require.NoError(t, array.Open(TILEDB_WRITE))

	q, err := NewQuery(tdbCtx, array)
	require.NoError(t, err)

	sa, err := array.NewSubarray()
	require.NoError(t, err)
	require.NoError(t, sa.AddRange(0, MakeRange[int32](1, 10)))
	require.NoError(t, sa.AddRange(1, MakeRange[int32](1, 10)))
	require.NoError(t, q.SetSubarray(sa))

	d0Label0 := []float64{-1.0, -0.8, -0.6, -0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8}
	d1Label0 := []float32{0.8, 0.6, 0.4, 0.2, 0, -0.2, -0.4, -0.6, -0.8, -1.0}

	vBuffer := make([]int32, 100)
	for i := int32(0); i < 100; i++ {
		vBuffer[i] = i + 1
	}

	_, err = q.SetDataBuffer("d0_label0", d0Label0)
	require.NoError(t, err)
	_, err = q.SetDataBuffer("d1_label0", d1Label0)
	require.NoError(t, err)
	_, err = q.SetDataBuffer("v", vBuffer)
	require.NoError(t, err)
	require.NoError(t, q.Submit())
	require.NoError(t, q.Finalize())

	// query with labels
	array, err = NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, array.Open(TILEDB_READ))

	q, err = NewQuery(tdbCtx, array)
	require.NoError(t, err)
	sa, err = array.NewSubarray()
	require.NoError(t, err)
	require.NoError(t, sa.AddDimensionLabelRange("d0_label0", MakeRange(0, 0.2)))
	require.NoError(t, sa.AddDimensionLabelRange("d1_label0", MakeRange[float32](-0.4, -0.2)))
	require.NoError(t, q.SetSubarray(sa))
	for i := range vBuffer {
		vBuffer[i] = 0
	}
	_, err = q.SetDataBuffer("v", vBuffer)
	require.NoError(t, err)
	require.NoError(t, q.Submit())
	require.NoError(t, q.Finalize())

	assert.Equal(t, int32(56), vBuffer[0])
	assert.Equal(t, int32(57), vBuffer[1])
	assert.Equal(t, int32(66), vBuffer[2])
	assert.Equal(t, int32(67), vBuffer[3])
	assert.Equal(t, int32(0), vBuffer[4])
}

func TestDimensionLabelSchema(t *testing.T) {
	schema := schemaSparseWithDimensionLabels(t)

	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	// create the array with the schema and read it back to verify the value
	uri := t.TempDir()
	memArray, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, memArray.Create(schema))

	diskArray, err := NewArray(tdbCtx, uri)
	require.NoError(t, err)
	require.NoError(t, diskArray.Open(TILEDB_READ))
	diskSchema, err := diskArray.Schema()
	require.NoError(t, err)

	num, err := diskSchema.DimensionLabelsNum()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), num)

	dimLabelCheck(t, schema, uri, 0, "d0_label0", TILEDB_FLOAT64, TILEDB_INCREASING_DATA, 1)
	dimLabelCheck(t, schema, uri, 1, "d0_label1", TILEDB_INT64, TILEDB_DECREASING_DATA, 1)
	dimLabelCheck(t, schema, uri, 2, "d1_label0", TILEDB_FLOAT32, TILEDB_DECREASING_DATA, 1)
}

// dimLabelCheck Retrieve a dimension label from schema by name and check expected values.
func dimLabelCheck(t *testing.T, schema *ArraySchema, arrayURI string, idx int, name string, labelType Datatype, labelOrder DataOrder, cellValNum uint32) {
	exists, err := schema.HasDimensionLabel(name)
	require.NoError(t, err)
	assert.True(t, exists)

	dimLabel, err := schema.DimensionLabelFromName(name)
	require.NoError(t, err)
	dimLabelType, err := dimLabel.Type()
	require.NoError(t, err)
	assert.Equal(t, labelType, dimLabelType)

	dimLabelOrder, err := dimLabel.Order()
	require.NoError(t, err)
	assert.Equal(t, labelOrder, dimLabelOrder)

	dimLabelName, err := dimLabel.Name()
	require.NoError(t, err)
	assert.Equal(t, name, dimLabelName)

	dimLabelUri, err := dimLabel.URI()
	require.NoError(t, err)
	assert.Contains(t, dimLabelUri, arrayURI)

	dimLabelAttrName, err := dimLabel.AttributeName()
	require.NoError(t, err)
	assert.Equal(t, "label", dimLabelAttrName)

	dimLabelCellValNum, err := dimLabel.CellValNum()
	require.NoError(t, err)
	assert.Equal(t, cellValNum, dimLabelCellValNum)

	dimLabelInd, err := schema.DimensionLabelFromIndex(uint64(idx))
	require.NoError(t, err)
	dimLabelIndUri, err := dimLabelInd.URI()
	require.NoError(t, err)
	assert.Equal(t, dimLabelUri, dimLabelIndUri)
}

func schemaSparseWithDimensionLabels(t *testing.T) *ArraySchema {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	d0, err := NewDimension(tdbCtx, "d0", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)
	d1, err := NewDimension(tdbCtx, "d1", TILEDB_INT32, []int32{1, 10}, int32(5))
	require.NoError(t, err)

	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(d0, d1))

	schema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	require.NoError(t, schema.SetDomain(domain))

	require.NoError(t, schema.AddDimensionLabel(0, "d0_label0", TILEDB_INCREASING_DATA, TILEDB_FLOAT64))
	require.NoError(t, schema.AddDimensionLabel(0, "d0_label1", TILEDB_DECREASING_DATA, TILEDB_INT64))
	require.NoError(t, schema.AddDimensionLabel(1, "d1_label0", TILEDB_DECREASING_DATA, TILEDB_FLOAT32))

	err = schema.SetDimensionLabelTileExtent("d0_label0", TILEDB_INT32, int32(2))
	require.NoError(t, err)

	filter, err := NewFilter(tdbCtx, TILEDB_FILTER_GZIP)
	require.NoError(t, err)
	require.NoError(t, filter.SetOption(TILEDB_COMPRESSION_LEVEL, int32(5)))
	filterList, err := NewFilterList(tdbCtx)
	require.NoError(t, err)
	require.NoError(t, filterList.AddFilter(filter))
	require.NoError(t, schema.SetDimensionLabelFilterList("d1_label0", *filterList))

	attr, err := NewAttribute(tdbCtx, "v", TILEDB_INT32)
	require.NoError(t, err)
	require.NoError(t, schema.AddAttributes(attr))

	return schema
}
