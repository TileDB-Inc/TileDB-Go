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
- macOS (OSX

### Prerequisites

This package requires the tiledb shared library be installed and on the system path. See the
[official tiledb installation instructions](https://docs.tiledb.com/developer/installation/quick-install)
for installation methods. TileDB must be compiled with serialization support enabled.

### Go Installation

For installation instructions please visit [Quick Install](https://docs.tiledb.com/developer/installation/quick-install) in the docs

## Quickstart

TileDB core documentation has a good
[quickstart guide](https://docs.tiledb.com/developer/quickstart) .
The two complete examples in the guide are
[quickstart_dense_test.go](https://docs.tiledb.com/developer/quickstart#dense-array) and
[quickstart_sparse_test.go](https://docs.tiledb.com/developer/quickstart#sparse-array).
More examples in the [examples](examples) folder demonstrate several features of 
the library.

## Compatibility

TileDB-Go follows semantic versioning. Currently tiledb core library does not,
as such the below table reference which versions are compatible.

| TileDB-Go Version | TileDB Version |
| ----------------- | -------------- |
| 0.1.X             | 1.3.X          |
| 0.2.X             | 1.4.X          |
| 0.3.X             | 1.4.X          |
| 0.4.X             | 1.5.X          |
| 0.5.X             | 1.5.X          |
| 0.6.X             | 1.6.X          |
| 0.7.X             | 1.6.X          |
| 0.8.X             | 1.7.X          |


## Missing Functionality

The following TileDB core library features are missing from the go api:

- TileDB generic object management
- TileDB group creation
