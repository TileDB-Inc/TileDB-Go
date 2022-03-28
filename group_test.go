//go:build experimental
// +build experimental

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
	group, err := NewGroup(context, tmpGroup)
	require.NoError(t, err)
	require.NoError(t, group.Create())

	// Creating the same group twice should error
	group, err = NewGroup(context, tmpGroup)
	require.NoError(t, err)
	assert.Error(t, group.Create())
}
