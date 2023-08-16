//go:build experimental

package tiledb

import "testing"

func TestObjectType(t *testing.T) {
	v := TILEDB_ARRAY
	s := v.String()

	got, err := ObjectTypeFromString(s)
	if err != nil {
		t.Errorf("unexpected error: %s", err)
	}

	if got != v {
		t.Errorf("got: %v not equal to input: %v", got, v)
	}

}
