package tiledb

import (
	"unsafe"

	// Much of this package relies on the fact that the Go GC is non-moving.
	_ "go4.org/unsafe/assume-no-moving-gc"
)

// Freeable represents an object that can be Free'd at the end of its lifetime
// to release its resources.
type Freeable interface {
	Free() // Releases non–garbage-collected resources held by this object.
}

// unsafeSlice creates a slice pointing at the given memory.
func unsafeSlice[T any](ptr unsafe.Pointer, length uint) []T {
	if ptr == nil {
		return nil
	}
	typedPtr := (*T)(ptr)
	return unsafe.Slice(typedPtr, length)
}
