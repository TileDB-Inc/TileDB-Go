package tiledb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubarrayForDenseArray(t *testing.T) {
	arrPath := createDenseIntegerGrid(t, 16)
	arr := openArray(t, arrPath, TILEDB_READ)

	t.Run("GetRange", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		rangeY, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeY, 0, 15)

		rangeX, err := sa.GetRange(1, 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeX, 0, 15)
	})

	t.Run("GetRangeFromName", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		rangeY, err := sa.GetRangeFromName("y", 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeY, 0, 15)

		rangeX, err := sa.GetRangeFromName("x", 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeX, 0, 15)
	})

	t.Run("AddRange", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		require.NoError(t, sa.AddRange(0, MakeRange[uint8](0, 3)))
		require.NoError(t, sa.AddRange(1, MakeRange[uint8](4, 7)))

		rangeY, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeY, 0, 3)

		rangeX, err := sa.GetRange(1, 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeX, 4, 7)
	})

	t.Run("AddRangeChecks", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		err = sa.AddRange(0, MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch, range: uint32 dimension: uint8")
	})

	t.Run("AddRangeByName", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		require.NoError(t, sa.AddRangeByName("y", MakeRange[uint8](0, 3)))
		require.NoError(t, sa.AddRangeByName("x", MakeRange[uint8](4, 7)))

		rangeY, err := sa.GetRangeFromName("y", 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeY, 0, 3)

		rangeX, err := sa.GetRangeFromName("x", 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeX, 4, 7)
	})

	t.Run("AddRangeByNameChecks", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		err = sa.AddRangeByName("y", MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch, range: uint32 dimension: uint8")
	})

	t.Run("AddRangeMixed", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		require.NoError(t, sa.AddRangeByName("y", MakeRange[uint8](0, 3)))
		require.NoError(t, sa.AddRange(1, MakeRange[uint8](4, 7)))

		rangeY, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeY, 0, 3)

		rangeX, err := sa.GetRangeFromName("x", 0)
		require.NoError(t, err)
		checkRange[uint8](t, rangeX, 4, 7)
	})
}

func TestSubarrayForSparseArray(t *testing.T) {
	arrPath := createSparse2dTable(t)
	arr := openArray(t, arrPath, TILEDB_READ)

	t.Run("GetRange", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		rangeS, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[string](t, rangeS, "", "")

		rangeN, err := sa.GetRange(1, 0)
		require.NoError(t, err)
		checkRange[float32](t, rangeN, 0.0, 100.0)
	})

	t.Run("GetRangeFromName", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		rangeS, err := sa.GetRangeFromName("str", 0)
		require.NoError(t, err)
		checkRange[string](t, rangeS, "", "")

		rangeN, err := sa.GetRangeFromName("num", 0)
		require.NoError(t, err)
		checkRange[float32](t, rangeN, 0.0, 100.0)
	})

	t.Run("AddRange", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		require.NoError(t, sa.AddRange(0, MakeRange("from", "to")))
		require.NoError(t, sa.AddRange(1, MakeRange[float32](2.0, 4.0)))

		rangeS, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[string](t, rangeS, "from", "to")

		rangeN, err := sa.GetRange(1, 0)
		require.NoError(t, err)
		checkRange[float32](t, rangeN, 2.0, 4.0)
	})

	t.Run("AddRangeChecks", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		err = sa.AddRange(0, MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dimension is of variable size but range is not")

		err = sa.AddRange(1, MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch, range: uint32 dimension: float32")
	})

	t.Run("AddRangeByName", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		require.NoError(t, sa.AddRangeByName("str", MakeRange("from", "to")))
		require.NoError(t, sa.AddRangeByName("num", MakeRange[float32](2.0, 4.0)))

		rangeS, err := sa.GetRange(0, 0)
		require.NoError(t, err)
		checkRange[string](t, rangeS, "from", "to")

		rangeN, err := sa.GetRange(1, 0)
		require.NoError(t, err)
		checkRange[float32](t, rangeN, 2.0, 4.0)
	})

	t.Run("AddRangeByNameChecks", func(t *testing.T) {
		sa, err := arr.NewSubarray()
		require.NoError(t, err)

		err = sa.AddRangeByName("str", MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "dimension is of variable size but range is not")

		err = sa.AddRangeByName("num", MakeRange[uint32](0x04040404, 0x07070707))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "mismatch, range: uint32 dimension: float32")
	})
}

func TestQueryWithSubarrayOnFixedDimensions(t *testing.T) {
	arrPath := createDenseIntegerGrid(t, 16)
	cfg, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(cfg)
	require.NoError(t, err)

	// write the full grid
	arr := openArray(t, arrPath, TILEDB_WRITE)
	q, err := NewQuery(tdbCtx, arr)
	require.NoError(t, err)
	sa, err := arr.NewSubarray()
	require.NoError(t, err)
	err = sa.AddRange(0, MakeRange[uint8](0, 15))
	require.NoError(t, err)
	err = sa.AddRange(1, MakeRange[uint8](0, 15))
	require.NoError(t, err)
	err = q.SetSubarray(sa)
	require.NoError(t, err)
	data := make([]uint16, 16*16)
	for i := range data {
		data[i] = uint16(i)
	}
	_, err = q.SetDataBuffer("v", data)
	require.NoError(t, err)
	err = q.Submit()
	require.NoError(t, err)

	// read back the central inner 2x2 subgrid
	arr = openArray(t, arrPath, TILEDB_READ)
	q, err = NewQuery(tdbCtx, arr)
	require.NoError(t, err)
	sa, err = arr.NewSubarray()
	require.NoError(t, err)
	err = sa.AddRange(0, MakeRange[uint8](7, 8))
	require.NoError(t, err)
	err = sa.AddRange(1, MakeRange[uint8](7, 8))
	require.NoError(t, err)
	err = q.SetSubarray(sa)
	require.NoError(t, err)
	data = make([]uint16, 2*2)
	_, err = q.SetDataBuffer("v", data)
	require.NoError(t, err)
	err = q.Submit()
	require.NoError(t, err)
	require.Equal(t, []uint16{119, 120, 135, 136}, data)
}

func TestQueryWithSubarrayOnVarDimensions(t *testing.T) {
	arrPath := createSparse2dTable(t)
	cfg, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(cfg)
	require.NoError(t, err)

	// write some values
	arr := openArray(t, arrPath, TILEDB_WRITE)
	q, err := NewQuery(tdbCtx, arr)
	require.NoError(t, err)
	_, err = q.SetDataBuffer("str", []byte("aaaabbbbccccdddd"))
	require.NoError(t, err)
	_, err = q.SetOffsetsBuffer("str", []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	require.NoError(t, err)
	_, err = q.SetDataBuffer("num", []float32{1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0})
	require.NoError(t, err)
	_, err = q.SetDataBuffer("v", []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	require.NoError(t, err)
	require.NoError(t, q.Submit())

	// read back the central inner 2x2 subgrid
	arr = openArray(t, arrPath, TILEDB_READ)
	q, err = NewQuery(tdbCtx, arr)
	require.NoError(t, err)
	sa, err := arr.NewSubarray()
	require.NoError(t, err)
	err = sa.AddRangeByName("str", MakeRange("b", "c"))
	require.NoError(t, err)
	err = sa.AddRangeByName("num", MakeRange[float32](2.0, 3.0))
	require.NoError(t, err)
	err = q.SetSubarray(sa)
	require.NoError(t, err)
	data := make([]uint16, 2*2)
	_, err = q.SetDataBuffer("v", data)
	require.NoError(t, err)
	err = q.Submit()
	require.NoError(t, err)
	require.Equal(t, []uint16{5.0, 6.0, 9.0, 10.0}, data)
}

func createDenseIntegerGrid(t *testing.T, n uint8) string {
	tileSize := uint8(4)
	if n < 4 {
		tileSize = n
	}
	arrPath := t.TempDir()

	cfg, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(cfg)
	require.NoError(t, err)
	schema, err := NewArraySchema(tdbCtx, TILEDB_DENSE)
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	yDim, err := NewDimension(tdbCtx, "y", TILEDB_UINT8, []uint8{0, n - 1}, tileSize)
	require.NoError(t, err)
	xDim, err := NewDimension(tdbCtx, "x", TILEDB_UINT8, []uint8{0, n - 1}, tileSize)
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(yDim, xDim))
	require.NoError(t, schema.SetDomain(domain))
	vAttr, err := NewAttribute(tdbCtx, "v", TILEDB_UINT16)
	require.NoError(t, err)
	require.NoError(t, schema.AddAttributes(vAttr))
	require.NoError(t, CreateArray(tdbCtx, arrPath, schema))
	return arrPath
}

func createSparse2dTable(t *testing.T) string {
	arrPath := t.TempDir()
	cfg, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(cfg)
	require.NoError(t, err)
	schema, err := NewArraySchema(tdbCtx, TILEDB_SPARSE)
	require.NoError(t, err)
	domain, err := NewDomain(tdbCtx)
	require.NoError(t, err)
	sDim, err := NewStringDimension(tdbCtx, "str")
	require.NoError(t, err)
	nDim, err := NewDimension(tdbCtx, "num", TILEDB_FLOAT32, []float32{0.0, 100.0}, float32(4.0))
	require.NoError(t, err)
	require.NoError(t, domain.AddDimensions(sDim, nDim))
	require.NoError(t, schema.SetDomain(domain))
	vAttr, err := NewAttribute(tdbCtx, "v", TILEDB_UINT16)
	require.NoError(t, err)
	require.NoError(t, schema.AddAttributes(vAttr))
	require.NoError(t, CreateArray(tdbCtx, arrPath, schema))
	return arrPath
}

func openArray(t *testing.T, arrPath string, qt QueryType) *Array {
	cfg, err := NewConfig()
	require.NoError(t, err)
	tdbCtx, err := NewContext(cfg)
	require.NoError(t, err)
	arr, err := NewArray(tdbCtx, arrPath)
	require.NoError(t, err)
	require.NoError(t, arr.Open(qt))
	t.Cleanup(func() { arr.Close() })
	return arr
}

func checkRange[T DimensionType](t *testing.T, r Range, start, end T) {
	bounds, err := ExtractRange[T](r)
	require.NoError(t, err)
	require.Equal(t, 3, len(bounds))
	assert.EqualValues(t, start, bounds[0])
	assert.EqualValues(t, end, bounds[1])
	assert.EqualValues(t, *new(T), bounds[2])
}
