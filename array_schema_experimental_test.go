package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArraySchemaAtTime tests creating an array schema at a provided timestamp.
func TestArraySchemaAtTime(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	arraySchema, err := NewArraySchema(context, TILEDB_DENSE)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	lo, hi, err := arraySchema.TimestampRange()
	require.NoError(t, err)

	domain, err := NewDomain(context)
	require.NoError(t, err)
	assert.NotNil(t, domain)
	dimension, err := NewDimension(context, "d1", TILEDB_UINT8, []uint8{1, 10}, uint8(5))
	require.NoError(t, err)
	assert.NotNil(t, dimension)
	require.NoError(t, domain.AddDimensions(dimension))

	attr, err := NewAttribute(context, "a1", TILEDB_UINT8)
	require.NoError(t, err)
	assert.NotNil(t, attr)

	require.NoError(t, arraySchema.SetDomain(domain))
	require.NoError(t, arraySchema.AddAttributes(attr))

	tempDir := t.TempDir()
	err = CreateArray(context, tempDir, arraySchema)
	require.NoError(t, err)

	arr, err := NewArray(context, tempDir)
	require.NoError(t, err)
	require.NotNil(t, arr)

	// Test we can open the array with timestamps given by ArraySchema.TimestampRange
	err = arr.OpenWithOptions(TILEDB_READ, WithStartTimestamp(lo), WithEndTimestamp(hi))
	require.NoError(t, err)

	// Opened Start and End timestamps should be equal to TimestampRange values.
	start, err := arr.OpenStartTimestamp()
	require.NoError(t, err)
	end, err := arr.OpenEndTimestamp()
	require.NoError(t, err)
	require.EqualValues(t, lo, start)
	require.EqualValues(t, hi, end)
}
