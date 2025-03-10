package tiledb

import (
	"unsafe"

	// Much of this package relies on the fact that the Go GC is non-moving.
	// When we move to a new Go version, this dependency should be updated
	// to ensure that the new version is still a non-moving GC.
	_ "go4.org/unsafe/assume-no-moving-gc"
)

// Freeable represents an object that can be Free'd at the end of its lifetime
// to release its resources.
type Freeable interface {
	Free() // Releases non–garbage-collected resources held by this object.
}

// freeFreeable frees the Freeable. It's free-floating to avoid capturing
// anything in a closure.
func freeFreeable(obj Freeable) { obj.Free() }

// unsafeSlice creates a slice pointing at the given memory.
func unsafeSlice[T any](ptr unsafe.Pointer, length uint) []T {
	if ptr == nil {
		return nil
	}
	typedPtr := (*T)(ptr)
	return unsafe.Slice(typedPtr, length)
}
