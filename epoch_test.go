package tiledb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEpoch(t *testing.T) {
	timeObject := GetTimeFromTimestamp(TILEDB_DATETIME_WEEK, 15)
	then := time.Date(1970, 4, 16, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_WEEK, -15)
	then = time.Date(1969, 9, 18, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_MONTH, 83)
	then = time.Date(1976, 11, 30, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_MONTH, -83)
	then = time.Date(1963, 2, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_YEAR, 15)
	then = time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_YEAR, -15)
	then = time.Date(1955, 1, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_NS, 1000000000)
	then = time.Date(1970, 1, 1, 0, 0, 1, 0, time.UTC)
	assert.Equal(t, then, timeObject)

	timeObject = GetTimeFromTimestamp(TILEDB_DATETIME_NS, -1000000000)
	then = time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC)
	assert.Equal(t, then, timeObject)
}
