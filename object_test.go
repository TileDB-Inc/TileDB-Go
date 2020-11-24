package tiledb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectCreate(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	assert.Nil(t, err)

	// create temp group name
	tmpObjectGroup := os.TempDir() + string(os.PathSeparator) + "tiledb_test_object_group"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpObjectGroup)
	if _, err = os.Stat(tmpObjectGroup); err == nil {
		os.RemoveAll(tmpObjectGroup)
	}

	// Create initial group
	err = GroupCreate(context, tmpObjectGroup)
	assert.Nil(t, err)

	obj, err := NewObject(context, tmpObjectGroup)
	assert.Nil(t, err)
	assert.NotNil(t, obj)

	objType, err := obj.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)
}
