# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://travis-ci.org/TileDB-Inc/TileDB-Go.svg?branch=master)](https://travis-ci.org/TileDB-Inc/TileDB-Go)

This package provides tiledb golang bindings via cgo. The bindings have been
designed to be idomatic go. `runtime.set_finalizer` is used to ensure proper
free'ing of c heap allocated structures

## Installation

### Supported Platforms

Currently the following platforms are supported:

- Linux
- macOS (OSX)

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
	a, _ := tiledb.NewAttribute(ctx, "a", tiledb.TILEDB_INT32)
	schema.AddAttributes(a)

	// Create the (empty) array on disk.
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Create(schema)
}

func writeDenseArray() {
	ctx, _ := tiledb.NewContext(nil)

	// Prepare some data for the array
	data := []int32{1, 2, 3}

	// Open the array for writing and create the query.
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Open(tiledb.TILEDB_WRITE)
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetBuffer("a", data)

	// Perform the write and close the array.
	query.Submit()
	array.Close()
}

func readDenseArray() {
	ctx, _ := tiledb.NewContext(nil)

	// Prepare the array for reading
	array, _ := tiledb.NewArray(ctx, denseArrayName)
	array.Open(tiledb.TILEDB_READ)

	// Prepare the vector that will hold the result (of size 6 elements)
	data := make([]int32, 3)

	// Prepare the query
	query, _ := tiledb.NewQuery(ctx, array)
	query.SetLayout(tiledb.TILEDB_ROW_MAJOR)
	query.SetBuffer("a", data)

	// Submit the query and close the array.
	query.Submit()
	array.Close()

	// Print out the results.
	fmt.Println(data)
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

- Key-Value store
