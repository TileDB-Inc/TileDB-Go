package examples_lib

import (
	"encoding/json"
	"fmt"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

func createArrayMetadataArray(dir string) {
	// Create a TileDB context.
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// The array will be 4x4 with dimensions "rows" and "cols",
	// with domain [1,4].
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

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, err := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_UINT32)
	checkError(err)
	defer a.Free()
	err = schema.AddAttributes(a)
	checkError(err)

	err = tiledb.CreateArray(ctx, dir, schema)
	checkError(err)
}

func writeArrayMetadata(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Open the array for writing and create the query.
	array, err := tiledb.NewArray(ctx, dir)
	defer array.Free()
	checkError(err)
	err = array.Open(tiledb.TILEDB_WRITE)
	defer array.Close()
	checkError(err)

	err = array.PutMetadata("key1", int32(25))
	checkError(err)

	err = array.PutMetadata("key2", []int32{25, 26, 27, 28})
	checkError(err)

	err = array.PutMetadata("key3", float32(25.1))
	checkError(err)

	err = array.PutMetadata("key4", []float32{25.1, 26.2, 27.3, 28.4})
	checkError(err)

	err = array.PutMetadata("key5", "This is TileDb array metadata")
	checkError(err)

	err = array.PutCharMetadata("key6", "This is TileDb array char metadata")
	checkError(err)
}

func readArrayMetadata(dir string) {
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

	dataType, valueNum, value, err := array.GetMetadata("key1")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.(int32))

	dataType, valueNum, value, err = array.GetMetadata("key2")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.([]int32))

	dataType, valueNum, value, err = array.GetMetadata("key3")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.(float32))

	dataType, valueNum, value, err = array.GetMetadata("key4")
	checkError(err)

	fmt.Printf("Datatype: %d\n", dataType)
	fmt.Printf("Value Num: %d\n", valueNum)
	fmt.Printf("Value: %v\n", value.([]float32))

	_, _, value, err = array.GetMetadata("key5")
	checkError(err)

	// String can be retrieved:
	fmt.Printf("Value: %v\n", value.(string))

	numOfMetadata, err := array.GetMetadataNum()
	checkError(err)

	fmt.Printf("Num of metadata: %d\n", numOfMetadata)

	arrayMetadata, err := array.GetMetadataFromIndex(0)
	checkError(err)

	fmt.Printf("Key: %s\n", arrayMetadata.Key)
	fmt.Printf("Key len: %d\n", arrayMetadata.KeyLen)
	fmt.Printf("Datatype: %d\n", arrayMetadata.Datatype)
	fmt.Printf("Value Num: %d\n", arrayMetadata.ValueNum)
	fmt.Printf("Value: %v\n", arrayMetadata.Value.(int32))

	var limit uint = 3
	arrayMetadataWithValueLimit, err := array.GetMetadataFromIndexWithValueLimit(5, &limit)
	checkError(err)

	fmt.Printf("Key: %s\n", arrayMetadataWithValueLimit.Key)
	fmt.Printf("Key len: %d\n", arrayMetadataWithValueLimit.KeyLen)
	fmt.Printf("Datatype: %d\n", arrayMetadataWithValueLimit.Datatype)
	fmt.Printf("Value Num: %d\n", arrayMetadata.ValueNum)
	fmt.Printf("Value: %v\n", arrayMetadataWithValueLimit.Value.(string))

	config, err := tiledb.NewConfig()
	checkError(err)
	defer config.Free()

	err = config.Set("sm.consolidation.mode", "array_meta")
	checkError(err)

	err = array.Consolidate(config)
	checkError(err)

	metadataMap, err := array.GetMetadataMap()
	checkError(err)

	jsonData, err := json.Marshal(metadataMap)
	checkError(err)

	fmt.Println(string(jsonData))
}

func clearArrayMetadata(dir string) {
	ctx, err := tiledb.NewContext(nil)
	checkError(err)
	defer ctx.Free()

	// Prepare the array for writing
	array, err := tiledb.NewArray(ctx, dir)
	checkError(err)
	defer array.Free()

	err = array.Open(tiledb.TILEDB_WRITE)
	checkError(err)
	defer array.Close()

	err = array.DeleteMetadata("key1")
	checkError(err)

	err = array.DeleteMetadata("key2")
	checkError(err)

	err = array.DeleteMetadata("key3")
	checkError(err)

	err = array.DeleteMetadata("key4")
	checkError(err)

	err = array.DeleteMetadata("key5")
	checkError(err)

	err = array.DeleteMetadata("key6")
	checkError(err)

	// Key does not exist
	err = array.DeleteMetadata("key7")
	checkError(err)
}

// RunArrayMetadataArray shows and example creation, writing and reading of a
// sparse array
func RunArrayMetadataArray() {
	tempDir := temp("metadata_array")
	defer cleanup(tempDir)

	createArrayMetadataArray(tempDir)
	writeArrayMetadata(tempDir)
	readArrayMetadata(tempDir)
	clearArrayMetadata(tempDir)
}
