package examples_lib

import (
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
const statsArrayName = "stats_array"

func createStatsArray(rowTileExtent uint32, colTileExtent uint32) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Define a domain
	domain, err := tiledb.NewDomain(ctx)
	checkError(err)
	defer domain.Free()

	rowDim, err := tiledb.NewDimension(ctx,
		"rows", tiledb.TILEDB_UINT32, []uint32{1, 12000}, rowTileExtent)
	checkError(err)
	defer rowDim.Free()

	colDim, err := tiledb.NewDimension(ctx,
		"cols", tiledb.TILEDB_UINT32, []uint32{1, 12000}, colTileExtent)
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
	array, err := tiledb.NewArray(ctx, statsArrayName)
	checkError(err)
	defer array.Free()

	err = array.Create(schema)
	checkError(err)
}

func writeStatsArray() {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare some data for the array
	values := make([]int32, 12000*12000)
	for i := 0; i < len(values); i++ {
		values[i] = int32(i)
	}

	// Create the query
	array, err := tiledb.NewArray(ctx, statsArrayName)
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
	_, err = query.SetBuffer("a", values)
	checkError(err)

	// Perform the write and close the array.
	err = query.Submit()
	checkError(err)

	err = query.Finalize()
	checkError(err)
}

func readStatsArray() {
	// Create TileDB context
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for reading
	array, err := tiledb.NewArray(ctx, statsArrayName)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_READ)
	checkError(err)
	defer array.Close()

	// Read a slice of 3,000 rows.
	subArray := []uint32{1, 3000, 1, 12000}

	// Prepare the query
	query, err := tiledb.NewQuery(ctx, array)
	checkError(err)
	defer query.Free()

	err = query.SetSubArray(subArray)
	checkError(err)

	// Prepare the vector that will hold the result
	bufferElements, err := query.EstimateBufferElements()
	checkError(err)
	data := make([]int32, bufferElements["a"][1])

	err = query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	checkError(err)
	_, err = query.SetBuffer("a", data)
	checkError(err)

	// Enable the stats for the read query
	err = tiledb.StatsEnable()
	checkError(err)

	// Submit the query
	err = query.Submit()
	checkError(err)

	// Print the report
	err = tiledb.StatsDumpSTDOUT()
	checkError(err)
	err = tiledb.StatsDisable()
	checkError(err)

	// Close the array
	err = query.Finalize()
	checkError(err)
}

func RunUsingTileDBStats() {
	createStatsArray(1, 12000)
	writeStatsArray()
	readStatsArray()

	// Cleanup example so unit tests are clean
	if _, err := os.Stat(statsArrayName); err == nil {
		err = os.RemoveAll(statsArrayName)
		checkError(err)
	}
}
