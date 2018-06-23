package tiledb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGroupCreate(t *testing.T) {

	// Test context without config
	context, err := NewContext(nil)
	assert.Nil(t, err)

	// create temp group name
	tmpGroup := os.TempDir() + string(os.PathSeparator) + "tiledb_test_group"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpGroup)
	if _, err = os.Stat(tmpGroup); err == nil {
		os.RemoveAll(tmpGroup)
	}

	// Create initial group
	err = GroupCreate(context, tmpGroup)
	assert.Nil(t, err)

	// Creating the same group twice should error
	err = GroupCreate(context, tmpGroup)
	assert.NotNil(t, err)
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
