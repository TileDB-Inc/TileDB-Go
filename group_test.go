package tiledb

import (
	"path/filepath"
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
	require.NoError(t, CreateGroup(context, tmpGroup))

	// Creating the same group twice should error
	assert.Error(t, CreateGroup(context, tmpGroup))

	// Test Group.IsOpen
	group, err := NewGroup(context, tmpGroup)
	require.NoError(t, err)
	isOpen, err := group.IsOpen()
	require.NoError(t, err)
	assert.False(t, isOpen)

	err = group.Open(TILEDB_WRITE)
	require.NoError(t, err)
	isOpen, err = group.IsOpen()
	require.NoError(t, err)
	assert.True(t, isOpen)

	queryType, err := group.QueryType()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_WRITE, queryType)

	err = group.Close()
	require.NoError(t, err)

	// Dump the created group
	err = group.Open(TILEDB_READ)
	require.NoError(t, err)
	isOpen, err = group.IsOpen()
	require.NoError(t, err)
	assert.True(t, isOpen)

	queryType, err = group.QueryType()
	require.NoError(t, err)
	assert.Equal(t, TILEDB_READ, queryType)

	dump, err := group.Dump(false)
	require.NoError(t, err)
	assert.NotEmpty(t, dump)
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

	// Verify fetching metadata with metadata map
	require.NoError(t, group.Open(TILEDB_READ))
	gmd, err := group.GetMetadataMap()
	require.NoError(t, err)
	require.Lenf(t, gmd, 1, "expected metadata map")
	require.Equal(t, gmd["key"], "value")
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

	// =========================================================================
	// Test adding members to the group
	t.Run("add members", func(t *testing.T) {
		group, err := createTestGroup(tdbCtx, t.TempDir())
		require.NoError(t, err)

		arraySchema := buildArraySchema(tdbCtx, t)

		arrayURI1, arrayURI2 := t.TempDir(), t.TempDir()
		require.NoError(t, addTwoArraysToGroup(tdbCtx, group, arraySchema, arrayURI1, arrayURI2))

		// verify we have two arrays
		count, err := memberCount(group)
		require.NoError(t, err)
		assert.EqualValues(t, uint(2), count)

		// Dump the created group
		err = group.Open(TILEDB_READ)
		require.NoError(t, err)

		dump, err := group.Dump(false)
		require.NoError(t, err)
		assert.NotEmpty(t, dump)
		assert.Contains(t, dump, arrayURI1)
		assert.Contains(t, dump, arrayURI2)
	})

	// Test adding members to the group with type
	t.Run("add members with type", func(t *testing.T) {
		group, err := createTestGroup(tdbCtx, t.TempDir())
		require.NoError(t, err)

		addMembersToGroupWithType(t, tdbCtx, group)
	})
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

	count, err = memberCount(group)
	require.NoError(t, err)
	require.EqualValues(t, uint64(1), count)

	require.NoError(t, group.Open(TILEDB_READ))
	uri, name, objectType, err := group.GetMemberFromIndex(0)
	require.NoError(t, err)
	assert.EqualValues(t, "file://"+arrayPathToKeep, uri)
	assert.EqualValues(t, objectType, TILEDB_ARRAY)
	assert.EqualValues(t, name, arrayPathToKeep)
	require.NoError(t, group.Close())
}

func TestGetMemberByName(t *testing.T) {
	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	group, err := createTestGroup(tdbCtx, t.TempDir())
	require.NoError(t, err)

	arraySchema := buildArraySchema(tdbCtx, t)
	arrayPath1, arrayPath2 := t.TempDir(), t.TempDir()
	require.NoError(t, addTwoArraysToGroup(tdbCtx, group, arraySchema, arrayPath1, arrayPath2))

	require.NoError(t, group.Open(TILEDB_READ))
	uri, name, objectType, err := group.GetMemberByName(arrayPath1)
	require.NoError(t, err)
	assert.EqualValues(t, "file://"+arrayPath1, uri)
	assert.EqualValues(t, objectType, TILEDB_ARRAY)
	assert.EqualValues(t, name, arrayPath1)
	require.NoError(t, group.Close())
}

func TestDeserializeGroup(t *testing.T) {
	tdbCtx, err := NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}

	buffer, err := NewBuffer(tdbCtx)
	if err != nil {
		t.Fatal(err)
	}

	groupDir := t.TempDir()

	require.NoError(t, CreateGroup(tdbCtx, groupDir))

	g, err := NewGroup(tdbCtx, groupDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := setConfigForWrite(g, 0); err != nil {
		t.Fatal(err)
	}

	require.NoError(t, g.Open(TILEDB_WRITE))
	if err := buffer.SetBuffer([]byte(`{
     "group": {
        "members": [
           {"uri": "tiledb://namespace/name", "type": "ARRAY", "name": "array1"},
           {"uri": "tiledb://namespace/name2", "type": "GROUP", "name": "group1"}
         ]
     }
}`)); err != nil {
		t.Fatal(err)
	}
	if err := g.Deserialize(buffer, TILEDB_JSON, true); err != nil {
		t.Fatalf("DeserializeGroup -> %v; expected no err", err)
	}
	require.NoError(t, g.Close())

	count, err := memberCount(g)
	require.NoError(t, err)
	require.EqualValues(t, uint64(2), count)
}

func TestGetIsRelativeURIByName(t *testing.T) {
	// create a group and add 2 members, one relative and one not
	groupURI := t.TempDir()
	arrayURI1 := t.TempDir()                       // for the non-relative member
	arrayURI2 := filepath.Join(groupURI, "array2") // for the relative member

	tdbCtx, err := NewContext(nil)
	require.NoError(t, err)

	group, err := createTestGroup(tdbCtx, groupURI)
	require.NoError(t, err)

	arraySchema := buildArraySchema(tdbCtx, t)
	err = CreateArray(tdbCtx, arrayURI1, arraySchema)
	require.NoError(t, err)
	err = CreateArray(tdbCtx, arrayURI2, arraySchema)
	require.NoError(t, err)

	require.NoError(t, group.Open(TILEDB_WRITE))
	require.NoError(t, group.AddMember(arrayURI1, "array1", false))
	require.NoError(t, group.AddMember("array2", "array2", true))
	require.NoError(t, group.Close())

	// check get relative of each member
	require.NoError(t, group.Open(TILEDB_READ))
	isRelative1, err := group.GetIsRelativeURIByName("array1")
	require.NoError(t, err)
	require.False(t, isRelative1)
	isRelative2, err := group.GetIsRelativeURIByName("array2")
	require.NoError(t, err)
	require.True(t, isRelative2)

	// check that non-existing members return error
	_, err = group.GetIsRelativeURIByName("array3")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error getting")

	require.NoError(t, group.Close())
}

func TestGroupDelete(t *testing.T) {
	// setup creates an hierarchy of groups and returns the
	// members URIs in the following order
	// outerGroup/
	//   outerArray
	//   innerGroup/
	//     innerArray
	setup := func(t *testing.T) []string {
		outerGroupURI := t.TempDir()
		outerArrayURI := t.TempDir()
		innerGroupURI := t.TempDir()
		innerArrayURI := t.TempDir()
		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)

		outerGroup, err := createTestGroup(tdbCtx, outerGroupURI)
		require.NoError(t, err)
		innerGroup, err := createTestGroup(tdbCtx, innerGroupURI)
		require.NoError(t, err)

		arraySchema := buildArraySchema(tdbCtx, t)
		err = CreateArray(tdbCtx, outerArrayURI, arraySchema)
		require.NoError(t, err)
		err = CreateArray(tdbCtx, innerArrayURI, arraySchema)
		require.NoError(t, err)

		require.NoError(t, innerGroup.Open(TILEDB_WRITE))
		require.NoError(t, innerGroup.AddMember(innerArrayURI, "innerArray", false))
		require.NoError(t, innerGroup.Close())
		require.NoError(t, outerGroup.Open(TILEDB_WRITE))
		require.NoError(t, outerGroup.AddMember(outerArrayURI, "outerArray", false))
		require.NoError(t, outerGroup.AddMember(innerGroup.uri, "innerGroup", false))
		require.NoError(t, outerGroup.Close())

		return []string{outerGroupURI, outerArrayURI, innerGroupURI, innerArrayURI}
	}

	t.Run("recursive", func(t *testing.T) {
		uris := setup(t)

		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)

		outerGroup, err := NewGroup(tdbCtx, uris[0])
		require.NoError(t, err)
		require.NoError(t, outerGroup.Open(TILEDB_MODIFY_EXCLUSIVE))
		require.NoError(t, outerGroup.Delete(true))

		tdbCfg, err := NewConfig()
		require.NoError(t, err)
		vfs, err := NewVFS(tdbCtx, tdbCfg)
		require.NoError(t, err)

		exists, err := vfs.IsFile(uris[0] + "/__tiledb_group.tdb")
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = vfs.IsFile(uris[2] + "/__tiledb_group.tdb")
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = vfs.IsDir(uris[1] + "/__schema")
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = vfs.IsDir(uris[1] + "/__schema")
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("nonrecursive", func(t *testing.T) {
		uris := setup(t)

		tdbCtx, err := NewContext(nil)
		require.NoError(t, err)

		outerGroup, err := NewGroup(tdbCtx, uris[0])
		require.NoError(t, err)
		require.NoError(t, outerGroup.Open(TILEDB_MODIFY_EXCLUSIVE))
		require.NoError(t, outerGroup.Delete(false))

		tdbCfg, err := NewConfig()
		require.NoError(t, err)
		vfs, err := NewVFS(tdbCtx, tdbCfg)
		require.NoError(t, err)

		exists, err := vfs.IsFile(uris[0] + "/__tiledb_group.tdb")
		require.NoError(t, err)
		require.False(t, exists)
		exists, err = vfs.IsFile(uris[2] + "/__tiledb_group.tdb")
		require.NoError(t, err)
		require.True(t, exists)

		dirSize, err := vfs.DirSize(uris[1] + "/__schema")
		require.NoError(t, err)
		require.True(t, dirSize > 0)
		dirSize, err = vfs.DirSize(uris[1] + "/__schema")
		require.NoError(t, err)
		require.True(t, dirSize > 0)
	})
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
	if err := CreateGroup(tdbCtx, uri); err != nil {
		return nil, err
	}

	// Create initial group
	group, err := NewGroup(tdbCtx, uri)
	if err != nil {
		return nil, err
	}

	return group, nil
}

func addTwoArraysToGroup(tdbCtx *Context, group *Group, arraySchema *ArraySchema, arrayURI1, arrayURI2 string) error {
	if err := CreateArray(tdbCtx, arrayURI1, arraySchema); err != nil {
		return err
	}

	if err := CreateArray(tdbCtx, arrayURI2, arraySchema); err != nil {
		return err
	}

	if err := setConfigForWrite(group, 0); err != nil {
		return err
	}

	if err := group.Open(TILEDB_WRITE); err != nil {
		return err
	}

	if err := group.AddMember(arrayURI1, arrayURI1, false); err != nil {
		return err
	}

	if err := group.AddMember(arrayURI2, arrayURI2, false); err != nil {
		return err
	}

	return group.Close()
}

func addMembersToGroupWithType(t *testing.T, tdbCtx *Context, group *Group) {
	err := group.Open(TILEDB_WRITE)
	require.NoError(t, err)

	// Group
	testGroup, err := createTestGroup(tdbCtx, t.TempDir())
	require.NoError(t, err)

	// Add Array member to group to test recursive Group.Dump
	testNestedArray := create1DTestArray(t)
	require.NoError(t, testGroup.Open(TILEDB_WRITE))
	err = testGroup.AddMemberWithType(testNestedArray.uri, "testNestedArray", false, TILEDB_ARRAY)
	require.NoError(t, err)
	require.NoError(t, testGroup.Close())

	err = group.AddMemberWithType(testGroup.uri, "testGroup", false, TILEDB_GROUP)
	require.NoError(t, err)

	// Array
	testArray := create1DTestArray(t)
	err = group.AddMemberWithType(testArray.uri, "testArray", false, TILEDB_ARRAY)
	require.NoError(t, err)

	require.NoError(t, group.Close())
	// Dump the created group
	err = group.Open(TILEDB_READ)
	require.NoError(t, err)

	dump, err := group.Dump(true)
	require.NoError(t, err)
	assert.NotEmpty(t, dump)
	assert.Contains(t, dump, "testGroup")
	assert.Contains(t, dump, "testNestedArray")
	assert.Contains(t, dump, "testArray")

	count, err := group.GetMemberCount()
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)
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
