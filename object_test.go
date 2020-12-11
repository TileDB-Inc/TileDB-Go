package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectCreate(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	assert.Nil(t, err)

	// create temp group name
	groupPath := filepath.Join(os.TempDir(), "tiledb_test_object_group")
	// Cleanup group when test ends
	defer os.RemoveAll(groupPath)
	if _, err = os.Stat(groupPath); err == nil {
		os.RemoveAll(groupPath)
	}

	groupPathNew := filepath.Join(os.TempDir(), "tiledb_test_object_group_move")
	// Cleanup group when test ends
	defer os.RemoveAll(groupPathNew)
	if _, err = os.Stat(groupPathNew); err == nil {
		os.RemoveAll(groupPathNew)
	}

	// Create initial group
	err = GroupCreate(context, groupPath)
	assert.Nil(t, err)

	objType, err := ObjectType(context, groupPath)
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)

	err = ObjectMove(context, groupPath, groupPathNew)
	assert.Nil(t, err)

	err = ObjectRemove(context, groupPathNew)
	assert.Nil(t, err)
}

func TestObjectArray(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	assert.Nil(t, err)

	// create temp group name
	groupPath := filepath.Join(os.TempDir(), "tiledb_test_object_group")
	// Cleanup group when test ends
	defer os.RemoveAll(groupPath)
	if _, err = os.Stat(groupPath); err == nil {
		os.RemoveAll(groupPath)
	}

	// Create initial group
	err = GroupCreate(context, groupPath)
	assert.Nil(t, err)

	arrayGroup := filepath.Join(groupPath, "arrays")

	// Create the array group
	err = GroupCreate(context, arrayGroup)
	assert.Nil(t, err)

	tmpArrayPath := filepath.Join(arrayGroup, "tiledb_test_array")

	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	objType, err := ObjectType(context, groupPath)
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)

	objectList, err := ObjectWalk(context, groupPath, TILEDB_PREORDER)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectTypeEnum)
	assert.Equal(t, TILEDB_ARRAY, objectList.objectList[1].objectTypeEnum)

	objectList, err = ObjectLs(context, groupPath)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectTypeEnum)
}
