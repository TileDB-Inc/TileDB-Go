/*
Package tiledb is a idomatic go binding to tiledb's c_api. Go structs are
used for object style access to tiledb types, such as Config and ArraySchema.
Tiledb c objects that are alloc'ed are set to be freeded on garbage collection
using `runtime.SetFinalizer`.

For more information on TileDB see official docs,
https://docs.tiledb.io/en/stable .

Semantic Versioning is followed for this package and for compatibility with
go modules. See compatibility section of Readme for a mapping of
TileDB-Go package to tiledb core library versions,
https://github.com/TileDB-Inc/TileDB-Go/blob/master/README.md#compatibility .


Installation

See readme for installation requirements and instructions:
https://github.com/TileDB-Inc/TileDB-Go/blob/master/README.md#installation .

Quickstart

See quickstart_dense_test.go and quickstart_sparse_test.go for examples. Also
checkout the official tiledb quickstart docs
https://docs.tiledb.io/en/latest/quickstart.html
*/
package tiledb
