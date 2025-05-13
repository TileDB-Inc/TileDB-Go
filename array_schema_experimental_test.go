package tiledb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArraySchemaAtTime tests creating an array schema at a provided timestamp.
func TestArraySchemaAtTime(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	context, err := NewContext(config)
	require.NoError(t, err)

	// Cannot create array schema at timestamp 0.
	arraySchema, err := NewArraySchemaAtTimestamp(context, TILEDB_DENSE, 0)
	require.Error(t, err)

	createTime := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	arraySchema, err = NewArraySchemaAtTime(context, TILEDB_DENSE, createTime)
	require.NoError(t, err)
	assert.NotNil(t, arraySchema)

	lo, hi, err := arraySchema.TimestampRange()
	require.NoError(t, err)

	require.EqualValues(t, lo, createTime.UnixMilli())
	require.EqualValues(t, hi, createTime.UnixMilli())
}
