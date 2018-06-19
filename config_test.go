package tiledb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

	assert.Nil(t, err)

	config.Free()
}

//TestSettingConfig
func TestSettingConfig(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	err = config.Set("sm.tile_cache_size", "fail")
	assert.NotNil(t, err)

	err = config.Set("sm.tile_cache_size", "10")
	assert.Nil(t, err)

	val, err := config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)
}

//TestGettingConfig
func TestGettingConfig(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)

	val, err := config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10000000", val)

	val, err = config.Get("sm.does_not_exists")
	assert.Nil(t, err)
	assert.Empty(t, val)
}

//TestUnSettingConfig
func TestUnSettingConfig(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	err = config.Set("sm.tile_cache_size", "10")
	assert.Nil(t, err)

	val, err := config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)

	err = config.Unset("sm.tile_cache_size")
	assert.Nil(t, err)

	val, err = config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10000000", val)
}

//TestFileConfig
func TestFileConfig(t *testing.T) {
	config, err := NewConfig()
	assert.Nil(t, err)
	assert.NotNil(t, config)
	err = config.Set("sm.tile_cache_size", "10")
	assert.Nil(t, err)

	val, err := config.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)

	// Create temporary path for testing configuration writing/reading
	tmpPath := os.TempDir() + string(os.PathSeparator) + "tiledb_test_config"
	defer os.Remove(tmpPath)
	if _, err = os.Stat(tmpPath); err == nil {
		os.Remove(tmpPath)
	}

	config.SaveToFile(tmpPath)

	config2, err := LoadConfig(tmpPath)
	assert.Nil(t, err)
	assert.NotNil(t, config2)

	val, err = config2.Get("sm.tile_cache_size")
	assert.Nil(t, err)
	assert.Equal(t, "10", val)
}
