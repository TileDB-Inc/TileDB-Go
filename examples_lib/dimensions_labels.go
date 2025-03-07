package examples_lib

import (
	"bytes"
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// createArray creates a 32x32 grid of uint32 and adds 1 dimension
// label to each dimension. One on floats and the other on strings
func createArrayWithDimensionLabels(uri string) {
	const (
		gridSize = 32
		tileSize = 8
	)

	tdbCfg := checkedValue(tiledb.NewConfig())
	tdbCtx := checkedValue(tiledb.NewContext(tdbCfg))

	// create the schema, add dimensions, attributes and dimension labels
	schema := checkedValue(tiledb.NewArraySchema(tdbCtx, tiledb.TILEDB_DENSE))
	checkError(schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR))
	checkError(schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR))
	domain := checkedValue(tiledb.NewDomain(tdbCtx))
	checkError(domain.AddDimensions(
		checkedValue(tiledb.NewDimension(tdbCtx, "y", tiledb.TILEDB_UINT16, []uint16{0, gridSize - 1}, uint16(tileSize))),
		checkedValue(tiledb.NewDimension(tdbCtx, "x", tiledb.TILEDB_UINT16, []uint16{0, gridSize - 1}, uint16(tileSize))),
	))
	checkError(schema.SetDomain(domain))
	checkError(schema.AddDimensionLabel(0, "y_str", tiledb.TILEDB_INCREASING_DATA, tiledb.TILEDB_STRING_ASCII))
	checkError(schema.AddDimensionLabel(1, "x_real", tiledb.TILEDB_INCREASING_DATA, tiledb.TILEDB_FLOAT32))
	checkError(schema.AddAttributes(checkedValue(tiledb.NewAttribute(tdbCtx, "v", tiledb.TILEDB_UINT32))))

	// create the array
	checkError(tiledb.CreateArray(tdbCtx, uri, schema))

	// set the dimension labels
	array := checkedValue(tiledb.NewArray(tdbCtx, uri))
	checkError(array.Open(tiledb.TILEDB_WRITE))

	q := checkedValue(tiledb.NewQuery(tdbCtx, array))

	// x_real labels are the dimension indices casted to floats
	// It is a fixed length label, a slice of float is sufficient
	xLabels := make([]float32, gridSize)
	for i := range xLabels {
		xLabels[i] = float32(i)
	}
	_ = checkedValue(q.SetDataBuffer("x_real", xLabels))

	// y_str labels are the dimension indices casted to fix length strings
	// It is a var length label, we need to record the data and the offsets
	strLength := len(fmt.Sprintf("%d", gridSize))
	yLabelsData := bytes.NewBuffer(make([]byte, 0, gridSize*strLength))
	yLabelsOffsets := make([]uint64, gridSize)
	for i := range yLabelsOffsets {
		_ = checkedValue(fmt.Fprintf(yLabelsData, "%0*d", strLength, i))
		yLabelsOffsets[i] = uint64(i * 2)
	}
	_ = checkedValue(q.SetDataBuffer("y_str", yLabelsData.Bytes()))
	_ = checkedValue(q.SetOffsetsBuffer("y_str", yLabelsOffsets))

	checkError(q.Submit())

	// fill the array with data. Each cell gets the row major rank
	q = checkedValue(tiledb.NewQuery(tdbCtx, array))
	sa := checkedValue(array.NewSubarray())
	checkError(sa.AddRangeByName("x", tiledb.MakeRange(uint16(0), uint16(gridSize-1))))
	checkError(sa.AddRangeByName("y", tiledb.MakeRange(uint16(0), uint16(gridSize-1))))
	checkError(q.SetSubarray(sa))
	b := make([]uint32, gridSize*gridSize)
	for i := range b {
		b[i] = uint32(i)
	}
	_ = checkedValue(q.SetDataBuffer("v", b))
	checkError(q.Submit())
}

func RunDimensionLabels() {
	tmpDir := temp("array_with_labels")
	defer cleanup(tmpDir)

	// create the array
	createArrayWithDimensionLabels(tmpDir)

	// prepare a tiledb context
	tdbCfg := checkedValue(tiledb.NewConfig())
	tdbCtx := checkedValue(tiledb.NewContext(tdbCfg))

	// query with labels
	array := checkedValue(tiledb.NewArray(tdbCtx, tmpDir))
	checkError(array.Open(tiledb.TILEDB_READ))

	sa := checkedValue(array.NewSubarray())
	checkError(sa.AddDimensionLabelRange("y_str", tiledb.MakeRange("01", "02")))
	checkError(sa.AddDimensionLabelRange("x_real", tiledb.MakeRange(float32(1.0), float32(2.0))))
	q := checkedValue(tiledb.NewQuery(tdbCtx, array))
	checkError(q.SetSubarray(sa))
	data := make([]uint32, 16)
	_ = checkedValue(q.SetDataBuffer("v", data))
	checkError(q.Submit())

	fmt.Println(data[0], data[1], data[2], data[3], data[4])
}
