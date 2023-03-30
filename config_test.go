package tiledb

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ExampleConfig_Set() {
	config, err := NewConfig()
	if err != nil {
		// handle error
	}

	err = config.Set("sm.memory_budget", "4294967296")
	if err != nil {
		// handle error
	}

	val, err := config.Get("sm.memory_budget")
	if err != nil {
		// handle error
	}
	fmt.Println(val)
	// Output: 4294967296
}

func ExampleConfig_Get() {
	config, err := NewConfig()
	if err != nil {
		// handle error
	}

	val, err := config.Get("sm.memory_budget")
	if err != nil {
		// handle error
	}
	fmt.Println(val)
	// Output: 5368709120
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
	assert.Error(t, config.Set("sm.memory_budget", "fail"))

	require.NoError(t, config.Set("sm.memory_budget", "4294967296"))

	val, err := config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "4294967296", val)
}

//TestGettingConfig
func TestGettingConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	val, err := config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "5368709120", val)

	val, err = config.Get("sm.does_not_exists")
	require.NoError(t, err)
	assert.Empty(t, val)
}

//TestUnSettingConfig
func TestUnSettingConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	require.NoError(t, config.Set("sm.memory_budget", "4294967296"))

	val, err := config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "4294967296", val)

	require.NoError(t, config.Unset("sm.memory_budget"))

	val, err = config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "5368709120", val)
}

//TestFileConfig
func TestFileConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	assert.NotNil(t, config)
	require.NoError(t, config.Set("sm.memory_budget", "4294967296"))

	val, err := config.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "4294967296", val)

	// Create temporary path for testing configuration writing/reading
	tmpPath := filepath.Join(t.TempDir(), "config")

	require.NoError(t, config.SaveToFile(tmpPath))

	config2, err := LoadConfig(tmpPath)
	require.NoError(t, err)
	assert.NotNil(t, config2)

	val, err = config2.Get("sm.memory_budget")
	require.NoError(t, err)
	assert.Equal(t, "4294967296", val)
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
