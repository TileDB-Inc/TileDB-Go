<a href="https://tiledb.com"><img src="https://github.com/TileDB-Inc/TileDB/raw/dev/doc/source/_static/tiledb-logo_color_no_margin_@4x.png" alt="TileDB logo" width="400"></a>

# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://dev.azure.com/TileDB-Inc/CI/_apis/build/status/TileDB-Inc.TileDB-Go?branchName=refs%2Fpull%2F123%2Fmerge)](https://dev.azure.com/TileDB-Inc/CI/_build/latest?definitionId=25&branchName=refs%2Fpull%2F123%2Fmerge)

This package provides [TileDB](https://github.com/TileDB-Inc/TileDB) golang bindings via cgo. The bindings have been
designed to be idomatic Go. `runtime.SetFinalizer` is used to ensure proper
free'ing of C heap allocated structures.

## Quick Links

- GoDoc API documentation: [https://pkg.go.dev/github.com/TileDB-Inc/TileDB-Go](https://pkg.go.dev/github.com/TileDB-Inc/TileDB-Go)
- Full Installation Docs: [https://docs.tiledb.com/main/how-to/installation](https://docs.tiledb.com/main/how-to/installation)
- Quick Install: [https://docs.tiledb.com/main/how-to/installation/quick-install](https://docs.tiledb.com/main/how-to/installation/quick-install)
- Full developer documentation for all APIs and integrations: [https://docs.tiledb.com](https://docs.tiledb.com)

## Installation

### Supported Platforms

Currently the following platforms are supported:

-   Linux
-   macOS (OSX)

### Prerequisites
This package requires the TileDB shared library be installed and on the system path. See the
[official TileDB installation instructions](https://docs.tiledb.com/main/how-to/installation)
for installation methods. TileDB must be compiled with serialization support enabled.

### Environment setup

Make sure you have Go installed on your system. This guide assumes you are using Go 1.17 or later, which fully supports
modules. You can check your Go version by running `go version`.

### Initialization steps

```bash
go mod init github.com/<github_username>/repository_name
```


### Go Installation

To install these bindings you can use `go get`:

```bash
 go get -v github.com/TileDB-Inc/TileDB-Go
```

To install package with test dependencies:

```bash
go get -v -t github.com/TileDB-Inc/TileDB-Go
```

### Go Testing

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
| 0.13.0            | >=2.4.X        |
| 0.14.0            | >=2.5.X        |
| 0.15.0            | >=2.8.X        |
| 0.16.0            | 2.10.X         |
| 0.17.0            | 2.11.X         |
| 0.18.0            | 2.12.X         |
| 0.19.0            | 2.13.X         |
| 0.20.0            | 2.14.X         |
| 0.21.0            | 2.15.X         |
| 0.22.0            | 2.16.X         |
| 0.23.0            | 2.17.X         |
| 0.24.0            | 2.18.X         |
| 0.25.0            | 2.19.X         |
| 0.26.0            | 2.20.X         |
| 0.27.0            | 2.21.X         |
| 0.28.0            | 2.22.X         |
| 0.29.0            | 2.23.X         |
| 0.30.0            | 2.24.X         |
| 0.31.0            | 2.25.X         |


## Deprecated Functionality

### 0.21.0

The query methods `(Set)?Buffer(Var|Nullable|Var|Unsafe)*` are deprecated because the corresponding
TileDB core methods are removed. The methods will be supported for 2 releases and are expected to be
removed in release 0.23. It is recommended to use the proper combination of
`(Set|Get)DataBuffer`, `(Set|Get)ValidityBuffer` and `(Set|Get)OffsetBuffer`.

### 0.23.1

The query methods `(Add|Get)?Range` are deprecated because they are deprecated in TileDB core.
It is recommend to use the `Subarray` type for building queries.
The methods will be removed in the release following their removal from TileDB core.

### 0.24.0

`Array.DeleteFragments` is deprecated in favor of `tiledb.DeleteFragments` which binds to
`C.tiledb_array_delete_fragments_v2` the preferred method to delete fragments in TileDB 2.18.0.

### 0.30.3

All deprecated APIs in TileDB-Go are removed as the corresopnding C-APIs will be removed in
the following TileDB release.