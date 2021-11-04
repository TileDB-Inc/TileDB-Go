package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Example usage of tiledb statistics
func ExampleStatsEnable() {

	err := StatsEnable()
	if err != nil {
		// Handle error
	}

	// Perform tile operations
	err = StatsDumpSTDOUT()
	if err != nil {
		// Handle error
	}
}

// Test statistics
func TestStats(t *testing.T) {
	// Enable statistics
	err := StatsEnable()
	require.NoError(t, err)

	// Reset all internal counters to 0
	require.NoError(t, StatsReset())

	// Dump statistics to stdout
	require.NoError(t, StatsDumpSTDOUT())

	tmpPath := filepath.Join(t.TempDir(), "dump")

	// Dump statistics to file
	require.NoError(t, StatsDump(tmpPath))

	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPath)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	// Dump statistics to existing file should error
	assert.Error(t, StatsDump(tmpPath))

	// Get statistics as string
	stats, err := Stats()
	require.NoError(t, err)
	assert.NotEmpty(t, stats)

	// Disable statistics
	require.NoError(t, StatsDisable())
}

// Test statistics
func TestStatsRaw(t *testing.T) {
	// Enable statistics
	err := StatsEnable()
	require.NoError(t, err)

	// Reset all internal counters to 0
	require.NoError(t, StatsReset())

	// Dump raw (json) statistics to stdout
	require.NoError(t, StatsRawDumpSTDOUT())

	tmpPath := filepath.Join(t.TempDir(), "dump")

	// Dump raw (json) statistics to file
	require.NoError(t, StatsRawDump(tmpPath))

	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPath)
	require.NoError(t, err)
	assert.NotZero(t, fileInfo.Size())

	// Dump raw (json) statistics to existing file should error
	err = StatsRawDump(tmpPath)
	assert.Error(t, err)

	// Get raw (json) statistics as string
	stats, err := StatsRaw()
	require.NoError(t, err)
	assert.NotEmpty(t, stats)

	// Disable statistics
	require.NoError(t, StatsDisable())
}
