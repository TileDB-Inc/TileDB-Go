<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://dev.azure.com/TileDB-Inc/CI/_apis/build/status/TileDB-Inc.TileDB-Go?branchName=refs%2Fpull%2F123%2Fmerge)](https://dev.azure.com/TileDB-Inc/CI/_build/latest?definitionId=25&branchName=refs%2Fpull%2F123%2Fmerge)

This package provides [TileDB](https://github.com/TileDB-Inc/TileDB) golang bindings via cgo. The bindings have been
designed to be idomatic Go. `runtime.SetFinalizer` is used to ensure proper
free'ing of C heap allocated structures.

## Quick Links

- Full Installation Docs: [https://docs.tiledb.com/main/how-to/installation](https://docs.tiledb.com/main/how-to/installation)
- Quickstart: [https://docs.tiledb.com/quickstart](https://docs.tiledb.com/main/solutions/tiledb-embedded/installation/quick-install)
- Full developer documentation for all APIs and integrations: [https://docs.tiledb.com](https://docs.tiledb.com)

## Installation

### Supported Platforms

Currently the following platforms are supported:

-   Linux
-   macOS (OSX)

### Prerequisites
This package requires the TileDB shared library be installed and on the system path. See the
[official TileDB installation instructions](https://docs.tiledb.com/main/solutions/tiledb-embedded/installation)
for installation methods. TileDB must be compiled with serialization support enabled.

### Go Installation

To install these bindings you can use `go get`:

```bash
 go get -v github.com/TileDB-Inc/TileDB-Go
```

To install package test dependencies:

```bash
go get -v -t github.com/TileDB-Inc/TileDB-Go
```

Package tests can be run with:

```bash
go test github.com/TileDB-Inc/TileDB-Go
```

## Compatibility

TileDB-Go follows semantic versioning. Currently TileDB core library does not,
as such the below table reference which versions are compatible.

| TileDB-Go Version | TileDB Version |
| ----------------- | -------------- |
| 0.7.X             | 1.6.X          |
| 0.8.0             | 1.7.0          |
| 0.8.1             | 1.7.0          |
| 0.8.2             | 1.7.2          |
| 0.8.3             | >=1.7.3        |
| 0.8.4             | >=1.7.3        |
| 0.8.5             | >=1.7.3        |
| 0.9.0             | 2.0.X          |
| 0.10.0            | 2.1.X          |
| 0.11.0            | 2.2.X          |
| 0.12.0            | 2.3.X          |


## Missing Functionality

The following TileDB core library features are missing from the Go API:

- TileDB generic object management
- TileDB group creation
