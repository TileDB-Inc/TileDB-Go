<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://travis-ci.org/TileDB-Inc/TileDB-Go.svg?branch=master)](https://travis-ci.org/TileDB-Inc/TileDB-Go)

This package provides tiledb golang bindings via cgo. The bindings have been
designed to be idomatic go. `runtime.set_finalizer` is used to ensure proper
free'ing of c heap allocated structures

## Quick Links

- Installation: [https://docs.tiledb.com/developer/installation](https://docs.tiledb.com/developer/installation)
- Quickstart: [https://docs.tiledb.com/developer/quickstart](https://docs.tiledb.com/developer/quickstart)
- Full developer documentation for all APIs and integrations: [https://docs.tiledb.com/developer](https://docs.tiledb.com/developer)

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
| 0.8.0             | 1.7.0          |
| 0.8.1             | 1.7.0          |
| 0.8.2             | 1.7.2          |


## Missing Functionality

The following TileDB core library features are missing from the go api:

- TileDB generic object management
- TileDB group creation
