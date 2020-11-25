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
	tmpObjectGroup := os.TempDir() + string(os.PathSeparator) +
		"tiledb_test_object_group"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpObjectGroup)
	if _, err = os.Stat(tmpObjectGroup); err == nil {
		os.RemoveAll(tmpObjectGroup)
	}

	tmpObjectGroupMove := os.TempDir() + string(os.PathSeparator) +
		"tiledb_test_object_group_move"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpObjectGroupMove)
	if _, err = os.Stat(tmpObjectGroupMove); err == nil {
		os.RemoveAll(tmpObjectGroupMove)
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

	err = obj.Move(tmpObjectGroupMove)
	assert.Nil(t, err)

	err = obj.Remove()
	assert.Nil(t, err)
}

func TestObjectArray(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	assert.Nil(t, err)

	// create temp group name
	tmpObjectGroup := os.TempDir() + string(os.PathSeparator) +
		"tiledb_test_object_group"
	// Cleanup group when test ends
	defer os.RemoveAll(tmpObjectGroup)
	if _, err = os.Stat(tmpObjectGroup); err == nil {
		os.RemoveAll(tmpObjectGroup)
	}

	// Create initial group
	err = GroupCreate(context, tmpObjectGroup)
	assert.Nil(t, err)

	arrayGroup := tmpObjectGroup + string(os.PathSeparator) + "arrays"

	// Create the array group
	err = GroupCreate(context, arrayGroup)
	assert.Nil(t, err)

	tmpArrayPath := arrayGroup + string(os.PathSeparator) + "tiledb_test_array"

	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	assert.Nil(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	err = array.Create(arraySchema)
	assert.Nil(t, err)

	obj, err := NewObject(context, tmpObjectGroup)
	assert.Nil(t, err)
	assert.NotNil(t, obj)

	objType, err := obj.Type()
	assert.Nil(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)

	objectList, err := obj.Walk(TILEDB_PREORDER)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectType)
	assert.Equal(t, TILEDB_ARRAY, objectList.objectList[1].objectType)

	objectList, err = obj.Ls()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectType)
}
