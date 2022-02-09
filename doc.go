/*
Package tiledb is an idiomatic Go binding to tiledb's c_api. Go structs are
used for object-style access to tiledb types, such as `Config` and `ArraySchema`.
Tiledb C objects that are alloc'ed are set to be freeded on garbage collection
using `runtime.SetFinalizer`.

For more information on TileDB see the official docs at
https://docs.tiledb.io/en/stable .

Semantic versioning is followed for this package and for compatibility with
Go modules. See the compatibility section of README.md for a mapping of
TileDB-Go package to tiledb core library versions,
https://github.com/TileDB-Inc/TileDB-Go/blob/master/README.md#compatibility .

Installation

See README.md for installation requirements and instructions:
https://github.com/TileDB-Inc/TileDB-Go/blob/master/README.md#installation .

Quickstart

See `quickstart_dense_test.go` and `quickstart_sparse_test.go` for examples. Also
check out the official tiledb quickstart docs at
https://docs.tiledb.io/en/latest/quickstart.html
*/
package tiledb
