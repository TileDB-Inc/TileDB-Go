package tiledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectCreate(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	require.NoError(t, err)

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
	require.NoError(t, GroupCreate(context, groupPath))

	objType, err := ObjectType(context, groupPath)
	require.NoError(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)

	require.NoError(t, ObjectMove(context, groupPath, groupPathNew))

	require.NoError(t, ObjectRemove(context, groupPathNew))
}

func TestObjectArray(t *testing.T) {
	// Create context
	context, err := NewContext(nil)
	require.NoError(t, err)

	// create temp group name
	groupPath := filepath.Join(os.TempDir(), "tiledb_test_object_group")
	// Cleanup group when test ends
	defer os.RemoveAll(groupPath)
	if _, err = os.Stat(groupPath); err == nil {
		os.RemoveAll(groupPath)
	}

	// Create initial group
	require.NoError(t, GroupCreate(context, groupPath))

	arrayGroup := filepath.Join(groupPath, "arrays")

	// Create the array group
	require.NoError(t, GroupCreate(context, arrayGroup))

	tmpArrayPath := filepath.Join(arrayGroup, "tiledb_test_array")

	// Create new array struct
	array, err := NewArray(context, tmpArrayPath)
	require.NoError(t, err)
	assert.NotNil(t, array)

	arraySchema := buildArraySchema(context, t)

	// Create array on disk
	require.NoError(t, array.Create(arraySchema))

	objType, err := ObjectType(context, groupPath)
	require.NoError(t, err)
	assert.Equal(t, TILEDB_GROUP, objType)

	objType, err = ObjectType(context, tmpArrayPath)
	require.NoError(t, err)
	assert.Equal(t, TILEDB_ARRAY, objType)

	objectList, err := ObjectWalk(context, groupPath, TILEDB_PREORDER)
	require.NoError(t, err)
	assert.Equal(t, 2, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectTypeEnum)
	assert.Equal(t, TILEDB_ARRAY, objectList.objectList[1].objectTypeEnum)

	objectList, err = ObjectLs(context, groupPath)
	require.NoError(t, err)
	assert.Equal(t, 1, len(objectList.objectList))
	assert.Equal(t, TILEDB_GROUP, objectList.objectList[0].objectTypeEnum)
}
