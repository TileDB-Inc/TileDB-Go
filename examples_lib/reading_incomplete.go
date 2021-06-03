package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
	"github.com/TileDB-Inc/TileDB-Go/bytesizes"
)

func createReadingIncompleteArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx, "rows", tiledb.TILEDB_INT32, []int32{1, 4}, int32(2))
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx, "cols", tiledb.TILEDB_INT32, []int32{1, 4}, int32(2))
	checkError(err)
	defer colDim.Free()

	err = domain.AddDimensions(rowDim, colDim)
	checkError(err)

	// The array will be sparse.
	schema, err := tiledb.NewArraySchema(ctx, tiledb.TILEDB_SPARSE)
	checkError(err)
	defer schema.Free()

	err = schema.SetDomain(domain)
	checkError(err)
	err = schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	err = schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)

	// Add an attribute "a1" so each (i,j) cell can store an integer.
	a1, err := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_INT32)
	checkError(err)
	defer a1.Free()

	err = schema.AddAttributes(a1)
	checkError(err)

	// Add an attribute "a2" so each (i,j) cell can store a string.
	a2, err := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_STRING_UTF8)
	checkError(err)
	defer a2.Free()

	err = a2.SetCellValNum(tiledb.TILEDB_VAR_NUM)
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

func writeReadingIncompleteArray(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	buffD1 := []int32{1, 2, 2}
	buffD2 := []int32{1, 1, 2}
	a1Data := []int32{1, 2, 3}
	a2Data := []byte("abbccc")
	a2Off := []uint64{0, 1, 3}

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

	err = query.SetLayout(tiledb.TILEDB_GLOBAL_ORDER)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)
	_, err = query.SetBuffer("rows", buffD1)
	checkError(err)
	_, err = query.SetBuffer("cols", buffD2)
	checkError(err)

	// Perform the write, finalize and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func reallocateBuffers(
	rows *[]int32,
	cols *[]int32,
	a1Data *[]int32,
	a2Off *[]uint64,
	a2Data *[]byte) {
	fmt.Println("Reallocating...")

	//// Note: this is a naive reallocation - you should handle
	//// reallocation properly depending on your application
	*rows = make([]int32, 2*len(*rows))
	*cols = make([]int32, 2*len(*cols))
	*a1Data = make([]int32, 2*len(*a1Data))
	*a2Off = make([]uint64, 2*len(*a2Off))
	*a2Data = make([]byte, 2*len(*a2Data))
}

func printResultsReadingIncomplete(
	rows []int32,
	cols []int32,
	a1Data []int32,
	a2Off []uint64,
	a2Data []byte,
	resultElMap map[string][3]uint64) {
	// Get the string sizes
	resultElA2Off := resultElMap["a2"][0]

	if resultElA2Off == 0 {
		return
	}

	fmt.Println("Printing results...")

	var a2StrSizes []uint64

	for i := 0; i < int(resultElA2Off)-1; i++ {
		a2StrSizes = append(a2StrSizes, a2Off[i+1]-a2Off[i])
	}

	resultA2DataSize := resultElMap["a2"][1] *
		bytesizes.Byte
	a2StrSizes = append(a2StrSizes,
		resultA2DataSize-a2Off[resultElA2Off-1])

	// Get the strings
	a2Str := make([][]byte, resultElA2Off)
	for i := 0; i < int(resultElA2Off); i++ {
		a2Str[i] = make([]byte, 0)
		for j := 0; j < int(a2StrSizes[i]); j++ {
			a2Str[i] = append(a2Str[i], a2Data[a2Off[i]])
		}
	}

	// Print the results
	resultNum := resultElA2Off // For clarity
	for r := 0; r < int(resultNum); r++ {
		i := rows[r]
		j := cols[r]
		a1 := a1Data[r]
		fmt.Printf("Cell (%d, %d), a1: %d, a2: %s\n",
			i, j, a1, string(a2Str[r]))
	}
}

func readReadingIncompleteArray(dir string) {
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

	// Read the entire array
	subArray := []int32{1, 4, 1, 4}

	// Prepare buffers such that the results **cannot** fit
	rows := make([]int32, 1)
	cols := make([]int32, 1)
	a1Data := make([]int32, 1)
	a2Off := make([]uint64, 1)
	a2Data := make([]byte, 1)

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)
	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a1", a1Data)
	checkError(err)
	_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
	checkError(err)
	_, err = query.SetBuffer("rows", rows)
	checkError(err)
	_, err = query.SetBuffer("cols", cols)
	checkError(err)

	var queryStatus tiledb.QueryStatus

	for {
		// Submit the query
		err = query.Submit()
		checkError(err)

		queryStatus, err = query.Status()
		checkError(err)

		// Print out the results.
		elements, err := query.ResultBufferElements()
		checkError(err)
		resultNum := elements["a1"][1]

		if queryStatus == tiledb.TILEDB_INCOMPLETE && resultNum == 0 {
			reallocateBuffers(&rows, &cols, &a1Data, &a2Off, &a2Data)
			_, err = query.SetBuffer("a1", a1Data)
			checkError(err)
			_, _, err = query.SetBufferVar("a2", a2Off, a2Data)
			checkError(err)
			_, err = query.SetBuffer("rows", rows)
			checkError(err)
			_, err = query.SetBuffer("cols", cols)
			checkError(err)
		} else {
			elements, err := query.ResultBufferElements()
			checkError(err)
			printResultsReadingIncomplete(
				rows, cols, a1Data, a2Off, a2Data, elements)
		}

		if queryStatus != tiledb.TILEDB_INCOMPLETE {
			break
		}
	}

	err = query.Finalize()
	checkError(err)
}

func RunReadingIncompleteArray() {
	tmpDir := temp("reading_incomplete_array")
	defer cleanup(tmpDir)

	createReadingIncompleteArray(tmpDir)
	writeReadingIncompleteArray(tmpDir)
	readReadingIncompleteArray(tmpDir)
}
