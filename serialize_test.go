package tiledb

import (
	"runtime"
	"runtime/debug"
	"testing"
)

func TestDeserializeArraySchemaGC(t *testing.T) {
	// Disable garbage collection for this test.
	// TODO: Pull this out into a "disableGC" helper using t.Cleanup
	// when we stop supporting Go 1.13.
	was := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(was)

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
