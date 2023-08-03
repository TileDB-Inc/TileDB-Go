package tiledb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRange(t *testing.T) {
	r := MakeRange(uint8(10), uint8(20))
	iBounds, err := ExtractRange[uint8](r)
	require.NoError(t, err)
	require.Equal(t, uint8(10), iBounds[0])
	require.Equal(t, uint8(20), iBounds[1])

	// extract for other types should fail
	_, err = ExtractRange[uint16](r)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot extract a range of uint8 to a slice of uint16")

	s := MakeRange("start", "end")
	sBounds, err := ExtractRange[string](s)
	require.NoError(t, err)
	require.Equal(t, "start", sBounds[0])
	require.Equal(t, "end", sBounds[1])

	// extract for other types should fail
	_, err = ExtractRange[uint16](s)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot extract a range of string to a slice of uint16")
}
