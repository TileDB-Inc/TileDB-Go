package examples_lib

import (
	"fmt"
	"os"
	"unsafe"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/array_wrapper"
)

// Name of array.
var sparseArrayName = "quickstart_sparse"

func createSparseArray() {
	dimMap := make(map[string]array_wrapper.DimensionDetail)
	dimMap["rows"] = array_wrapper.DimensionDetail{
		Domain: []int32{1, 4},
		Extent: int32(4),
	}
	dimMap["cols"] = array_wrapper.DimensionDetail{
		Domain: []int32{1, 4},
		Extent: int32(4),
	}

	attrMap := make(map[string]array_wrapper.AttributeDetail)
	attrMap["a"] = array_wrapper.AttributeDetail{
		Datatype: tiledb.TILEDB_UINT32,
	}

	// Create the (empty) array on disk.
	_, err := array_wrapper.NewSparseArray(sparseArrayName,
		tiledb.TILEDB_ROW_MAJOR, tiledb.TILEDB_ROW_MAJOR, dimMap, attrMap)
	checkError(err)
}

func writeSparseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 4, 3}
	data := []uint32{1, 2, 3}

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, sparseArrayName)
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
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readSparseArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, sparseArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetLayout(tiledb.TILEDB_UNORDERED)
	checkError(err)
	err = query.AddRange(0, int32(1), int32(2))
	checkError(err)
	err = query.AddRange(1, int32(1), int32(4))
	checkError(err)

	size, err := query.EstResultSize("a")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for attribute 'a': %d\n", *size)
	buffAR := make([]uint32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	size, err = query.EstResultSize("rows")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'rows': %d\n", *size)
	buffD1R := make([]int32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	size, err = query.EstResultSize("cols")
	checkError(err)
	fmt.Printf("Estimated query size in bytes for dimension 'cols': %d\n", *size)
	buffD2R := make([]int32, (*size)/uint64(unsafe.Sizeof(int32(0))))

	_, err = query.SetBuffer("rows", buffD1R)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2R)
	checkError(err)
	_, err = query.SetBuffer("a", buffAR)
	checkError(err)

	// Submit the query and close the array.
	err = query.Submit()
	checkError(err)

	for i, aVal := range buffAR {
		fmt.Printf("Cell (%d, %d) has data %d\n", buffD1R[i], buffD2R[i], aVal)
	}

	err = query.Finalize()
	checkError(err)
}

// RunSparseArray shows and example creation, writing and reading of a
// sparse array
func RunSparseArray() {
	createSparseArray()
	writeSparseArray()
	readSparseArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(sparseArrayName); err == nil {
		err = os.RemoveAll(sparseArrayName)
		checkError(err)
	}
}
