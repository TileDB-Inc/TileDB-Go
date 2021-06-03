package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createMultiAttributeArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4}, int32(4))
	checkError(err)
	defer colDim.Free()

	err = domain.AddDimensions(rowDim, colDim)
	checkError(err)

	// The array will be dense.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Create two attributes "a1" and "a2", so each (i,j) cell can store
	// a character on "a1" and a vector of two floats on "a2".
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_STRING_ASCII)
	checkError(err)
	defer a1.Free()

	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_FLOAT32)
	checkError(err)
	defer a2.Free()

	err = schema.AddAttributes(a1)
	checkError(err)
	err = a2.SetCellValNum(2)
	checkError(err)
	err = schema.AddAttributes(a2)
	checkError(err)

	// Create the (empty) array on disk.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeMultiAttributeArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	a1 := []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
		'm', 'n', 'o', 'p'}
	a2 := []float32{1.1, 1.2, 2.1, 2.2, 3.1, 3.2, 4.1, 4.2,
		5.1, 5.2, 6.1, 6.2, 7.1, 7.2, 8.1, 8.2,
		9.1, 9.2, 10.1, 10.2, 11.1, 11.2, 12.1, 12.2,
		13.1, 13.2, 14.1, 14.2, 15.1, 15.2, 16.1, 16.2}

	// Create the query
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1)
	checkError(err)
	_, err = query.SetBuffer("a2", a2)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readMultiAttributeArray(dir string) {
	// Create TileDB context
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

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	// (of size 6 elements for "a1" and 12 elements for "a2" since
	// it stores two floats per cell)
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	a1Data := make([]byte, bufferElements["a1"][1])
	a2Data := make([]float32, bufferElements["a2"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, err = query.SetBuffer("a2", a2Data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)

	fmt.Println("Reading both attributes a1 and a2:")
	for i := 0; i < int(bufferElements["a1"][1]); i++ {
		fmt.Printf("a1: %s, a2: (%.1f,%.1f)\n", string(a1Data[i]),
			a2Data[2*i], a2Data[2*i+1])
	}
	fmt.Printf("\n")
}

func readMultiAttributeArraySubSelect(dir string) {
	// Create TileDB context
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

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	// (of size 6 elements for "a1")
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	a1Data := make([]byte, bufferElements["a1"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)

	fmt.Println("Subselecting on attribute a1:")
	for i := 0; i < int(bufferElements["a1"][1]); i++ {
		fmt.Printf("a1: %s\n", string(a1Data[i]))
	}
	fmt.Printf("\n")
}

func RunMultiAttributeArray() {
	tmpDir := temp("multi_attribute_array")
	defer cleanup(tmpDir)

	createMultiAttributeArray(tmpDir)
	writeMultiAttributeArray(tmpDir)
	readMultiAttributeArray(tmpDir)
	readMultiAttributeArraySubSelect(tmpDir)
}
