package examples_lib

import (
	"fmt"
	"time"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createTimestampArray(dir string) {
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

func writeTimestampArray(dir string, key string, value string, timestamp uint64, bias int32) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	data := []int32{
		1, 2 + bias, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.OpenWithOptions(tiledb.TILEDB_WRITE, tiledb.WithEndTimestamp(timestamp))
	checkError(err)
	defer array.Close()

	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	fmt.Printf("Writing %s: %s\n", key, value)
	err = array.PutMetadata(key, value)
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func writeTimestampArrayMeta(dir string, key string, value string, timestamp uint64) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Open the array for writing
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.OpenWithOptions(tiledb.TILEDB_WRITE, tiledb.WithEndTimestamp(timestamp))
	checkError(err)
	defer array.Close()

	fmt.Printf("Writing %s: %s\n", key, value)
	err = array.PutMetadata(key, value)
	checkError(err)
}

func readTimestampArray(dir string, timestamp uint64) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.OpenWithOptions(tiledb.TILEDB_READ, tiledb.WithEndTimestamp(timestamp))
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

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()

	err = subarray.SetSubArray(subArray)
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	_, _, value, err := array.GetMetadata("meta_key")
	checkError(err)

	// String can be retrieved:
	fmt.Printf("Value: %v\n", value.(string))

	err = query.Finalize()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func readTimestampArrayWithOptions(dir string, timestamp uint64) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.OpenWithOptions(tiledb.TILEDB_READ, tiledb.WithEndTimestamp(timestamp))
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

	// Prepare the subarray
	subarray, err := array.NewSubarray()
	checkError(err)
	defer subarray.Free()

	err = subarray.SetSubArray(subArray)
	checkError(err)
	err = query.SetSubarray(subarray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetDataBuffer("a", data)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	_, _, value, err := array.GetMetadata("meta_key")
	checkError(err)

	// String can be retrieved:
	fmt.Printf("Value: %v\n", value.(string))

	err = query.Finalize()
	checkError(err)

	// Print out the results.
	fmt.Println(data)
}

func getTimestamp() uint64 {
	return uint64(time.Now().UTC().UnixMilli())
}

// RunTimestampArray shows timestamp correlation of written data and metadata
func RunTimestampArray() {
	tmpDir1 := temp("timestamp_array_1")
	defer cleanup(tmpDir1)

	createTimestampArray(tmpDir1)
	// Write data and metadata
	t1 := getTimestamp()
	writeTimestampArray(tmpDir1, "meta_key", "Write1", t1, 0)
	time.Sleep(5 * time.Millisecond)
	// Write metadata only
	t2 := getTimestamp()
	writeTimestampArrayMeta(tmpDir1, "meta_key", "Write2", t2)
	time.Sleep(5 * time.Millisecond)
	// Write metadata only
	t3 := getTimestamp()
	writeTimestampArrayMeta(tmpDir1, "meta_key", "Write3", t3)
	time.Sleep(5 * time.Millisecond)
	// Write metadata only
	t4 := getTimestamp()
	writeTimestampArrayMeta(tmpDir1, "meta_key", "Write4", t4)
	readTimestampArray(tmpDir1, t1)
	readTimestampArray(tmpDir1, t2)
	readTimestampArray(tmpDir1, t3)
	readTimestampArrayWithOptions(tmpDir1, t4)

	tmpDir2 := temp("timestamp_array_2")
	defer cleanup(tmpDir2)

	// Writing data and metadata
	createTimestampArray(tmpDir2)
	t1 = getTimestamp()
	writeTimestampArray(tmpDir2, "meta_key", "Write1", t1, 0)
	time.Sleep(5 * time.Millisecond)
	t2 = getTimestamp()
	writeTimestampArray(tmpDir2, "meta_key", "Write2", t2, 1)
	time.Sleep(5 * time.Millisecond)
	t3 = getTimestamp()
	writeTimestampArray(tmpDir2, "meta_key", "Write3", t3, 2)
	time.Sleep(5 * time.Millisecond)
	t4 = getTimestamp()
	writeTimestampArray(tmpDir2, "meta_key", "Write4", t4, 3)
	readTimestampArray(tmpDir2, t1)
	readTimestampArray(tmpDir2, t2)
	readTimestampArray(tmpDir2, t3)
	readTimestampArray(tmpDir2, t4)
}
