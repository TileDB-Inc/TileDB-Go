package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupCreate(t *testing.T) {

	// Test context without config
	context, err := NewContext(nil)
	require.NoError(t, err)

	// create temp group name
	tmpGroup := t.TempDir()

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
