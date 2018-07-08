# TileDB Go Bindings

[![GoDoc](https://godoc.org/github.com/TileDB-Inc/TileDB-Go?status.svg)](http://godoc.org/github.com/TileDB-Inc/TileDB-Go)
[![Build Status](https://travis-ci.org/TileDB-Inc/TileDB-Go.svg?branch=master)](https://travis-ci.org/TileDB-Inc/TileDB-Go)

This package provides tiledb golang bindings via cgo. The bindings have been
designed to be idomatic go. `runtime.set_finalizer` is used to ensure proper
free'ing of c heap allocated structures

## Installation

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
| 1.X.Y             | 1.3.0          |

## Example Usage

Below is a small example using vfs functionality. Additional examples are
provided in the GoDoc documentation.

```golang

// Create a new config
config, err := tiledb.NewConfig()
if err != nil {
  return err
}
// Optionally set config settings here
// config.Set("key", "value")

// Create a context
context, err := tiledb.NewContext(config)
if err != nil {
  return err
}

// Create a VFS instance
vfs, err := tiledb.NewVFS(context, config)
if err != nil {
  return err
}
```
