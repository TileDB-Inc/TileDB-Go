package tiledb

import (
	"runtime"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MemoryBuffer struct {
	bytes []byte
}

func (x *MemoryBuffer) Write(buff []byte) (int, error) {
	x.bytes = append(x.bytes, buff...)
	return len(buff), nil
}

func TestSerializeArraySchemaGC(t *testing.T) {
	disableGC(t)

	ctx, err := NewContext(nil)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := NewArraySchema(ctx, TILEDB_DENSE)
	if err != nil {
		t.Fatal(err)
	}
	dom, err := NewDomain(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dim, err := NewDimension(ctx, "d1", TILEDB_INT32, []int32{1, 10}, int32(2))
	if err != nil {
		t.Fatal(err)
	}
	if err := dom.AddDimensions(dim); err != nil {
		t.Fatal(err)
	}
	if err := schema.SetDomain(dom); err != nil {
		t.Fatal(err)
	}
	attr, err := NewAttribute(ctx, "a1", TILEDB_INT32)
	if err != nil {
		t.Fatal(err)
	}
	schema.AddAttributes(attr)
	buffer, err := SerializeArraySchemaToBuffer(schema, TILEDB_CAPNP, true)
	if err != nil {
		t.Fatal(err)
	}
	bytes := &MemoryBuffer{}
	_, err = buffer.WriteTo(bytes)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotEmpty(t, bytes.bytes)
	runtime.GC()
	runtime.GC()
}

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
