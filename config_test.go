package tiledb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleConfig_Set() {
	config, err := NewConfig()
	if err != nil {
		// handle error
	}

	err = config.Set("sm.tile_cache_size", "10")
	if err != nil {
		// handle error
	}

	val, err := config.Get("sm.tile_cache_size")
	if err != nil {
		// handle error
	}
	fmt.Println(val)
	// Output: 10
}

func ExampleConfig_Get() {
	config, err := NewConfig()
	if err != nil {
		// handle error
	}

	val, err := config.Get("sm.tile_cache_size")
	if err != nil {
		// handle error
	}
	fmt.Println(val)
	// Output: 10000000
}

func TestNewConfig(t *testing.T) {
	config, err := NewConfig()

	require.NoError(t, err)

	config.Free()
}

//TestSettingConfig
func TestSettingConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	assert.Error(t, config.Set("sm.tile_cache_size", "fail"))

	require.NoError(t, config.Set("sm.tile_cache_size", "10"))

	val, err := config.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10", val)
}

//TestGettingConfig
func TestGettingConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	val, err := config.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10000000", val)

	val, err = config.Get("sm.does_not_exists")
	require.NoError(t, err)
	assert.Empty(t, val)
}

//TestUnSettingConfig
func TestUnSettingConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	require.NoError(t, config.Set("sm.tile_cache_size", "10"))

	val, err := config.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10", val)

	require.NoError(t, config.Unset("sm.tile_cache_size"))

	val, err = config.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10000000", val)
}

//TestFileConfig
func TestFileConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	assert.NotNil(t, config)
	require.NoError(t, config.Set("sm.tile_cache_size", "10"))

	val, err := config.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10", val)

	// Create temporary path for testing configuration writing/reading
	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_config"
	defer os.Remove(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.Remove(tmpPath)
	}

	require.NoError(t, config.SaveToFile(tmpPath))

	config2, err := LoadConfig(tmpPath)
	require.NoError(t, err)
	assert.NotNil(t, config2)

	val, err = config2.Get("sm.tile_cache_size")
	require.NoError(t, err)
	assert.Equal(t, "10", val)
}

//TestConfigIter
func TestConfigIter(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	assert.NotNil(t, config)

	// Iterate the configuration
	iter, err := config.Iterate("vfs.s3.")
	require.NoError(t, err)
	assert.NotNil(t, iter)

	for ; !iter.IsDone(); err = iter.Next() {
		if err != nil {
			// handle error from call to iter.Next()
			return
		}
		// Get current param, value from iterator
		param, value, err := iter.Here()
		if err != nil {
			// handle error
			return
		}
		fmt.Printf("%s: %s\n", *param, *value)
	}

	// Output:
	// aws_access_key_id:
	// aws_secret_access_key:
	// aws_session_token:
	// ca_file:
	// ca_path:
	// connect_max_tries: 5
	// connect_scale_factor: 25
	// connect_timeout_ms: 3000
	// endpoint_override:
	// logging_level: Off
	// max_parallel_ops: 4
	// multipart_part_size: 5242880
	// proxy_host:
	// proxy_password:
	// proxy_port: 0
	// proxy_scheme: http
	// proxy_username:
	// region: us-east-1
	// request_timeout_ms: 3000
	// scheme: https
	// use_multipart_upload: true
	// use_virtual_addressing: true
	// verify_ssl: true
}
