package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.Nil(t, err)

	// Reset all internal counters to 0
	err = StatsReset()
	assert.Nil(t, err)

	// Dump statistics to stdout
	err = StatsDumpSTDOUT()
	assert.Nil(t, err)

	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_stats_test"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.RemoveAll(tmpPath)
	}

	// Dump statistics to file
	err = StatsDump(tmpPath)
	assert.Nil(t, err)

	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPath)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	// Dump statistics to existing file should error
	err = StatsDump(tmpPath)
	assert.NotNil(t, err)

	// Get statistics as string
	stats, err := Stats()
	assert.Nil(t, err)
	assert.NotEmpty(t, stats)

	// Disable statistics
	err = StatsDisable()
	assert.Nil(t, err)
}

// Test statistics
func TestStatsRaw(t *testing.T) {
	// Enable statistics
	err := StatsEnable()
	assert.Nil(t, err)

	// Reset all internal counters to 0
	err = StatsReset()
	assert.Nil(t, err)

	// Dump raw (json) statistics to stdout
	err = StatsRawDumpSTDOUT()
	assert.Nil(t, err)

	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_stats_test"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.RemoveAll(tmpPath)
	}

	// Dump raw (json) statistics to file
	err = StatsRawDump(tmpPath)
	assert.Nil(t, err)

	// Validate dumped file is non-empty
	fileInfo, err := os.Stat(tmpPath)
	assert.Nil(t, err)
	assert.NotZero(t, fileInfo.Size())

	// Dump raw (json) statistics to existing file should error
	err = StatsRawDump(tmpPath)
	assert.NotNil(t, err)

	// Get raw (json) statistics as string
	stats, err := StatsRaw()
	assert.Nil(t, err)
	assert.NotEmpty(t, stats)

	// Disable statistics
	err = StatsDisable()
	assert.Nil(t, err)
}
