package tiledb

import (
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
	groupPath := t.TempDir()

	groupPathNew := t.TempDir()

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
	groupPath := t.TempDir()

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
	assert.Equal(t, 2, len(objectList.ObjectList))
	assert.Equal(t, TILEDB_GROUP, objectList.ObjectList[0].ObjectTypeEnum)
	assert.Equal(t, TILEDB_ARRAY, objectList.ObjectList[1].ObjectTypeEnum)

	objectList, err = ObjectLs(context, groupPath)
	require.NoError(t, err)
	assert.Equal(t, 1, len(objectList.ObjectList))
	assert.Equal(t, TILEDB_GROUP, objectList.ObjectList[0].ObjectTypeEnum)
}
