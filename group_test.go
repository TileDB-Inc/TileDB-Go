package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupCreate(t *testing.T) {

	// Test context without config
	context, err := NewContext(nil)
	require.NoError(t, err)

	// create temp group name
	tmpGroup := os.TempDir() + string(os.PathSeparator) + "tiledb_test_group"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpGroup)
	if _, err = os.Stat(tmpGroup); err == nil {
		os.RemoveAll(tmpGroup)
	}

	// Create initial group
	require.NoError(t, GroupCreate(context, tmpGroup))

	// Creating the same group twice should error
	assert.Error(t, GroupCreate(context, tmpGroup))
}

func ExampleGroupCreate() {
	// Create context without config
	context, err := NewContext(nil)
	if err != nil {
		// Handle error
		return
	}

	// Create Group
	err = GroupCreate(context, "my_group")
	if err != nil {
		// Handle error
		return
	}
}
