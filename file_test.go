//go:build experimental
// +build experimental

package tiledb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"path"
	"testing"
)

func TestFile(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	assert.NotNil(t, config)

	// Test context with config
	context, err := NewContext(config)
	require.NoError(t, err)
	assert.NotNil(t, context)

	tmpFilePath := path.Join(os.TempDir(), "tiledb_file_t.txt")
	f, err := os.Create(tmpFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFilePath)

	_, err = f.WriteString("simple text")
	require.NoError(t, err)

	tmpArrayPath := path.Join(os.TempDir(), "tiledb_test_array")
	defer os.RemoveAll(tmpArrayPath)
	if _, err = os.Stat(tmpArrayPath); err == nil {
		os.RemoveAll(tmpArrayPath)
	}

	file, err := NewFile(context, tmpArrayPath)
	require.NoError(t, err)

	err = file.CreateDefault()
	require.Error(t, err)

	err = file.SetConfig(config)
	require.NoError(t, err)

	err = file.CreateDefault()
	require.NoError(t, err)
}
