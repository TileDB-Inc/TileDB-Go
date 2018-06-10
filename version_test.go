package tiledb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVersion(t *testing.T) {
	major, minor, rev := Version()

	assert.True(t, major > -1)
	assert.True(t, minor > -1)
	assert.True(t, rev > -1)
}

func ExampleVersion() {
	major, minor, rev := Version()
	fmt.Printf("TileDB shared library version is %d.%d.%d", major, minor, rev)
}
