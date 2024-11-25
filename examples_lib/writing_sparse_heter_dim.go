package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createSparseHeterDimArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	d1, err := tiledb.NewDimension(ctx, "d1", tiledb.TILEDB_FLOAT32, []float32{1.0, 20.0}, float32(5.0))
	checkError(err)
	defer d1.Free()

	d2, err := tiledb.NewDimension(ctx, "d2", tiledb.TILEDB_INT64, []int64{1, 30}, int64(5))
	checkError(err)
	defer d2.Free()

	err = domain.AddDimensions(d1, d2)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
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

func writeSparseHeterDimArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Write some simple data to cells.
	buffD1 := []float32{1.1, 1.2, 1.3, 1.4}
	buffD2 := []int64{1, 2, 3, 4}
	buffA := []int32{1, 2, 3, 4}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	_, err = query.SetDataBuffer("d1", buffD1)
	checkError(err)
	_, err = query.SetDataBuffer("d2", buffD2)
	checkError(err)
	_, err = query.SetDataBuffer("a", buffA)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readSparseHeterDimArray(dir string) {
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

	// Non-empty domain: [1,4], [1,4]
	x, isEmpty, err := array.NonEmptyDomain()
	checkError(err)
	if !isEmpty {
		d1 := x[0].Bounds.([]float32)
		d2 := x[1].Bounds.([]int64)
		fmt.Printf("Non-empty domain: [%f,%f], [%d,%d]\n",
			d1[0], d1[1], d2[0], d2[1])
	}

	buffD1R := make([]float32, 4)
	buffD2R := make([]int64, 4)
	buffAR := make([]int32, 4)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()
	err = subarray.AddRange(0, tiledb.MakeRange[float32](1.0, 20.0))
	checkError(err)
	err = subarray.AddRange(1, tiledb.MakeRange[int64](1, 30))
	checkError(err)

	_, err = query.SetDataBuffer("d1", buffD1R)
	checkError(err)
	_, err = query.SetDataBuffer("d2", buffD2R)
	checkError(err)
	_, err = query.SetDataBuffer("a", buffAR)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	fmt.Printf("D1 Buffer: %v\n", buffD1R)
	fmt.Printf("D2 Buffer: %v\n", buffD2R)
	fmt.Printf("A Attribute Data: %v\n", buffAR)

	err = query.Finalize()
	checkError(err)
}

// RunSparseHeterDimArray shows and example creation, writing and reading of
// a sparse array using heterogeneus dimensions
func RunSparseHeterDimArray() {
	tmpDir := temp("sparse_heter_dim_array")
	defer cleanup(tmpDir)

	createSparseHeterDimArray(tmpDir)
	writeSparseHeterDimArray(tmpDir)
	readSparseHeterDimArray(tmpDir)
}
