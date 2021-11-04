package tiledb

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestDeserializeArraySchemaGC(t *testing.T) {
	disableGC(t)

	ctx, err := NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := NewBuffer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := buffer.SetBuffer([]byte(`invalid`)); err != nil {
		t.Fatal(err)
	}
	if schema, err := DeserializeArraySchema(buffer, TILEDB_CAPNP, true); err == nil {
		t.Fatalf("DeserializeArraySchema(bogus JSON) -> %v; want err", schema)
	}
	runtime.GC()
	runtime.GC()
}

// disableGC disables garbage collection for the duration of a test.
func disableGC(t testing.TB) {
	t.Helper()
	was := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(was) })
}
