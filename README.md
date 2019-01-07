# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://travis-ci.org/TileDB-Inc/TileDB-Go.svg?branch=master)](https://travis-ci.org/TileDB-Inc/TileDB-Go)

This package provides tiledb golang bindings via cgo. The bindings have been
designed to be idomatic go. `runtime.set_finalizer` is used to ensure proper
free'ing of c heap allocated structures

## Installation

### Supported Platforms

Currently the following platforms are supported:

-   Linux
-   macOS (OSX)

### Prerequisites
This package requires the tiledb shared library be installed and on the system path. See the
[official tiledb installation instructions](https://docs.tiledb.io/en/stable/installation.html)
for installation methods.

### Go Installation

To install these bindings you can use go get:

```bash
 go get -v github.com/TileDB-Inc/TileDB-Go
```

To install package test dependencies:

```bash
go get -vt github.com/TileDB-Inc/TileDB-Go
```

Package tests can be run with:

```bash
go test github.com/TileDB-Inc/TileDB-Go
```

## Compatibility

TileDB-Go follows semantic versioning. Currently tiledb core library does not,
as such the below table reference which versions are compatible.

| TileDB-Go Version | TileDB Version |
| ----------------- | -------------- |
| 0.1.X             | 1.3.0          |
| 0.2.X             | 1.4.0          |
| 0.3.X             | 1.4.0          |
| 0.4.X             | 1.5.0 (Unreleased) |

## Quickstart

TileDB core documentation has a good
[quickstart guide](https://docs.tiledb.io/en/latest/quickstart.html) .
The two complete examples in the guide are
[quickstart_dense_test.go](quickstart_dense_test.go) and
[quickstart_sparse_test.go](quickstart_sparse_test.go).

## Example Usage

Below is a small example for writing and reading  a dense 1 dimensional
array. For simplicity error handling is ignored in the example.
Additional examples are provided in the GoDoc documentation.

```golang
package main

import (
	"fmt"
	"os"

	tiledb "github.com/TileDB-Inc/TileDB-Go"
)

// Name of array.
var denseArrayName = "quickstart_dense"

func createDenseArray() {
	// Create a TileDB context.
	ctx, _ := tiledb.NewContext(nil)

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	domain, _ := tiledb.NewDomain(ctx)
	rowDim, _ := tiledb.NewDimension(ctx, "rows", []int32{1, 3}, int32(3))
	domain.AddDimensions(rowDim)

	// The array will be dense.
	schema, _ := tiledb.NewArraySchema(ctx, tiledb.TILEDB_DENSE)
	schema.SetDomain(domain)
	schema.SetCellOrder(tiledb.TILEDB_ROW_MAJOR)
	schema.SetTileOrder(tiledb.TILEDB_ROW_MAJOR)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a, _ := tiledb.NewAttribute(ctx, "a1", tiledb.TILEDB_INT32)
	schema.AddAttributes(a)

	// Add a single attribute "a" so each (i,j) cell can store an integer.
	a2, _ := tiledb.NewAttribute(ctx, "a2", tiledb.TILEDB_CHAR)
	a2.SetCellValNum(TILEDB_VAR_NUM)
	schema.AddAttributes(a, a2)

	// Create the (empty) array on disk.
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Create(schema)
}

func writeDenseArray() {
	ctx, _ := tiledb.NewContext(nil)

	// Prepare some data for the array
	a1Data := []int32{1, 2, 3}

	// String attributes are handled as byte arrays
	// The user must pass a byte array to query.SetBuffer/SetBufferVar
	a2Data := []byte("val1" + "val2" + "val3")
	a2DataOffsets := []uint64{0,4,8}

	// Open the array for writing and create the query.
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Open(tiledb.TILEDB_WRITE)
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetBuffer("a1", a1Data)
	query.SetBufferVar("a2", a2Offsets, a2Data)

	// Perform the write and close the array.
	query.Submit()
	array.Close()
}

func readDenseArray() {
	ctx, _ := tiledb.NewContext(nil)

	// Prepare the array for reading
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Open(tiledb.TILEDB_READ)

	// Prepare the vector that will hold the result (of size 3 elements)
	// You can use Array.MaxBufferSize(subarray) to get estimate buffer sizes
	// The sizes are set here for simplicity of the example
	a1Data := make([]int32, 3)
	a2Offsets:= make([]uint64, 3)
	a2Data := make([]byte, 12)

	// Prepare the query
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetBuffer("a1", a1Data)
	query.SetVarBuffer("a2", a2Offsets, a2Data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(a1Data)
	fmt.Println(a2Data)
	fmt.Println(a2Offsets)

	// Produce slice of strings based on the offsets for a2
	// This also converts from a byte array to strings
	var a2Strings []string
	for int i := 0; i < len(a2Offsets); i++ {
		stringEndPosition := len(a2Data)
		if i < len(a2Offsets) - 1 {
			stringEndPosition = a2Offsets[i+1]
		}
		a2Strings = append(a2String, string(a2Data[a2Offsets[i]:stringEndPosition]))
		}

	fmt.Println(a2Strings)
}

// ExampleDenseArray shows and example creation, writing and reading of a dense
// array
func main() {
	createDenseArray()
	writeDenseArray()
	readDenseArray()
	// Output: [2 3 4 6 7 8]
}
```

## Missing Functionality

The following TileDB core library features are missing from the go api:

-   TileDB generic object management
-   TileDB group creation
