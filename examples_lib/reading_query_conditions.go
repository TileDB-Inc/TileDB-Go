package examples_lib

import (
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// RunQueryConditionsArray shows how query conditions work
func RunQueryConditionsArray() {
	tmpDir1 := temp("query_conditions_array_1")
	defer cleanup(tmpDir1)

	createReadingSparseLayoutsArray(tmpDir1)
	writeReadingSparseLayoutsArray(tmpDir1)
	readReadingSparseLayoutsArrayWithConditions(tmpDir1)
}

func readReadingSparseLayoutsArrayWithConditions(dir string) {
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
		rows := x[0].Bounds.([]int32)
		cols := x[1].Bounds.([]int32)
		fmt.Printf("Non-empty domain: [%d,%d], [%d,%d]\n",
			rows[0], rows[1], cols[0], cols[1])
	}

	// Slice only rows 1, 2 and cols 2, 3, 4
	subArray := []int32{1, 2, 2, 4}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)

	data := make([]uint32, bufferElements["a"][1])
	rows := make([]int32, bufferElements["rows"][1])
	cols := make([]int32, bufferElements["cols"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)
	_, err = query.SetBuffer("rows", rows)
	checkError(err)
	_, err = query.SetBuffer("cols", cols)
	checkError(err)

	qc, err := tiledb.NewQueryCondition(ctx, "a", tiledb.TILEDB_QUERY_CONDITION_EQ, uint32(2))
	checkError(err)

	err = query.SetQueryCondition(qc)
	checkError(err)

	var queryStatus tiledb.QueryStatus

	for { // Submit the query and close the array.
		err = query.Submit()
		checkError(err)

		queryStatus, err = query.Status()
		checkError(err)

		// Print out the results.
		elements, err := query.ResultBufferElements()
		checkError(err)
		resultNum := elements["a"][1]
		for r := 0; r < int(resultNum); r++ {
			i := rows[r]
			j := cols[r]
			a := data[r]
			fmt.Printf("Cell (%d, %d) has data %d\n", i, j, a)
		}

		if queryStatus != tiledb.TILEDB_INCOMPLETE {
			break
		}
	}

	err = query.Finalize()
	checkError(err)
}
