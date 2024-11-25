package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createStringDimArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	d, err := tiledb.NewStringDimension(ctx, "d")
	checkError(err)
	defer d.Free()

	err = domain.AddDimensions(d)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	defer a.Free()

	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeStringDimArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Open the array for writing
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	// Prepare some data for the array
	buffA := []int32{3, 2, 1, 4}
	dData := []byte("ccbbddddaa")
	dOff := []uint64{0, 2, 4, 8}

	// Create the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	_, err = query.SetDataBuffer("d", dData)
	checkError(err)
	_, err = query.SetOffsetsBuffer("d", dOff)
	checkError(err)
	_, err = query.SetDataBuffer("a", buffA)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readStringDimArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	nonEmptyDomain, isEmpty, err := array.NonEmptyDomainVarFromName("d")
	checkError(err)

	if !isEmpty {
		fmt.Printf("NonEmptyDomain Dimension Name: %v\n", nonEmptyDomain.DimensionName)
		fmt.Printf("NonEmptyDomain Bounds: %v\n", nonEmptyDomain.Bounds)
	}

	nonEmptyDomain, isEmpty, err = array.NonEmptyDomainVarFromIndex(uint(0))
	checkError(err)

	if !isEmpty {
		fmt.Printf("NonEmptyDomain Dimension Name: %v\n", nonEmptyDomain.DimensionName)
		fmt.Printf("NonEmptyDomain Bounds: %v\n", nonEmptyDomain.Bounds)
	}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()

	s1 := "a"
	s2 := "ee"
	err = subarray.AddRange(0, tiledb.MakeRange(s1, s2))
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)

	offsets := make([]uint64, 4)
	data := make([]byte, 10)
	_, err = query.SetDataBuffer("d", data)
	checkError(err)
	_, err = query.SetOffsetsBuffer("d", offsets)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	fmt.Printf("offsets: %v\n", offsets)
	fmt.Printf("data: %s\n", string(data))

	err = query.Finalize()
	checkError(err)
}

// RunStringDimArray shows an example of creation, writing and reading of a
// sparse array with string dim
func RunStringDimArray() {
	tmpDir := temp("string_dim_array")
	defer cleanup(tmpDir)

	createStringDimArray(tmpDir)
	writeStringDimArray(tmpDir)
	readStringDimArray(tmpDir)
}
