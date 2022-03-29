//go:build experimental
// +build experimental

package tiledb

import (
	"strconv"
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

func TestGroups_Metadata(t *testing.T) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	group, err := createTestGroup(tdbCtx, t.TempDir())
	require.NoError(t, err)

	// =========================================================================
	// Test adding metadata
	require.NoError(t, setConfigForWrite(group, 0))
	require.NoError(t, group.Open(TILEDB_WRITE))
	require.NoError(t, group.PutMetadata("key", "value"))
	require.NoError(t, group.Close())

	// =========================================================================
	// Verify it is added
	require.NoError(t, group.Open(TILEDB_READ))
	num, err := group.GetMetadataNum()
	require.NoError(t, err)
	assert.EqualValues(t, uint64(1), num)

	dType, _, val, err := group.GetMetadata("key")
	require.NoError(t, err)
	assert.EqualValues(t, dType, TILEDB_STRING_UTF8)
	assert.EqualValues(t, val, "value")
	require.NoError(t, group.Close())

	// =========================================================================
	// Remove it
	require.NoError(t, setConfigForWrite(group, 1))
	require.NoError(t, group.Open(TILEDB_WRITE))
	err = group.DeleteMetadata("key")
	require.NoError(t, err)
	require.NoError(t, group.Close())

	require.NoError(t, group.Open(TILEDB_READ))
	num, err = group.GetMetadataNum()
	require.NoError(t, err)
	assert.EqualValues(t, uint64(0), num)
	require.NoError(t, group.Close())
}

func TestGroups_AddMembers(t *testing.T) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	group, err := createTestGroup(tdbCtx, t.TempDir())
	require.NoError(t, err)

	// =========================================================================
	// Test adding members to the group
	arraySchema := buildArraySchema(tdbCtx, t)
	require.NoError(t, addTwoArraysToGroup(tdbCtx, group, arraySchema, t.TempDir(), t.TempDir()))

	// verify we have two arrays
	count, err := memberCount(group)
	require.NoError(t, err)
	assert.EqualValues(t, uint(2), count)
}

func TestGroups_RemoveMembers(t *testing.T) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	group, err := createTestGroup(tdbCtx, t.TempDir())
	require.NoError(t, err)

	arraySchema := buildArraySchema(tdbCtx, t)
	arrayPathToKeep, arrayPathToRemove := t.TempDir(), t.TempDir()
	require.NoError(t, addTwoArraysToGroup(tdbCtx, group, arraySchema, arrayPathToKeep, arrayPathToRemove))

	// verify we have two arrays
	count, err := memberCount(group)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)

	// =========================================================================
	// Remove the members and validate
	require.NoError(t, setConfigForWrite(group, 1))
	require.NoError(t, group.Open(TILEDB_WRITE))
	require.NoError(t, group.RemoveMember(arrayPathToRemove))
	require.NoError(t, group.Close())

	require.NoError(t, group.Open(TILEDB_READ))
	require.NoError(t, group.Close())

	count, err = memberCount(group)
	require.NoError(t, err)
	require.EqualValues(t, uint64(1), count)

	uri, objectType, err := group.GetMemberFromIndex(1)
	require.NoError(t, err)
	assert.EqualValues(t, uri, arrayPathToKeep)
	assert.EqualValues(t, objectType, TILEDB_ARRAY)
}

func memberCount(group *Group) (uint64, error) {
	if err := group.Open(TILEDB_READ); err != nil {
		return 0, err
	}
	count, err := group.GetMemberCount()
	if err != nil {
		return 0, err
	}

	if err := group.Close(); err != nil {
		return 0, err
	}

	return count, nil
}

func createTestGroup(tdbCtx *Context, uri string) (*Group, error) {
	// Create initial group
	group, err := NewGroup(tdbCtx, uri)
	if err != nil {
		return nil, err
	}

	if err := group.Create(); err != nil {
		return nil, err
	}
	return group, nil
}

func addTwoArraysToGroup(tdbCtx *Context, group *Group, arraySchema *ArraySchema, arrayURI1, arrayURI2 string) error {
	array1, err := NewArray(tdbCtx, arrayURI1)
	if err != nil {
		return err
	}

	if err := array1.Create(arraySchema); err != nil {
		return err
	}

	array2, err := NewArray(tdbCtx, arrayURI2)
	if err != nil {
		return err
	}

	if err := array2.Create(arraySchema); err != nil {
		return err
	}

	if err := setConfigForWrite(group, 0); err != nil {
		return err
	}

	if err := group.Open(TILEDB_WRITE); err != nil {
		return err
	}

	if err := group.AddMember(array1.uri, false); err != nil {
		return err
	}

	if err := group.AddMember(array2.uri, false); err != nil {
		return err
	}

	return group.Close()
}

func setConfigForWrite(group *Group, i int) error {
	conf, err := NewConfig()
	if err != nil {
		return err
	}
	if err := conf.Set("sm.group.timestamp_end", strconv.Itoa(1648581656+i)); err != nil {
		return err
	}

	if err := group.SetConfig(conf); err != nil {
		return err
	}
	return nil
}
