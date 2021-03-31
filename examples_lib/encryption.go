package examples_lib

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var encryptedArrayName = "encrypted_array"

// The 256-bit encryption key, stored as a string for convenience.
var encryption_key = "0123456789abcdeF0123456789abcdeF"

func createEncryptedArray() {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", []int32{1, 4}, int32(4))
	checkError(err)
	colDim, err := tiledb.NewDimension(ctx, "cols", []int32{1, 4}, int32(4))
	checkError(err)
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

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	checkError(err)
	defer a.Free()

	err = schema.AddAttributes(a)
	checkError(err)

	// Create the (empty) encrypted array with AES-256-GCM.
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	defer array.Free()

	err = array.CreateWithKey(schema, tiledb.TILEDB_AES_256_GCM, encryption_key)
	checkError(err)
}

func writeEncryptedArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	defer array.Free()

	err = array.OpenWithKey(tiledb.TILEDB_WRITE, tiledb.TILEDB_AES_256_GCM,
		encryption_key)
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)
}

func readEncryptedArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, encryptedArrayName)
	checkError(err)
	defer array.Free()

	err = array.OpenWithKey(tiledb.TILEDB_READ, tiledb.TILEDB_AES_256_GCM,
		encryption_key)
	checkError(err)
	defer array.Close()

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 6)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func RunEncryptedArray() {
	createEncryptedArray()
	writeEncryptedArray()
	readEncryptedArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(encryptedArrayName); err == nil {
		err = os.RemoveAll(encryptedArrayName)
		checkError(err)
	}
}
