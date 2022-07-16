//go:build asan
// +build asan

package main

// #cgo CFLAGS: -fsanitize=address
// #cgo LDFLAGS: -fsanitize=address
// void __lsan_do_leak_check(void);
import "C"

import (
	"runtime"
)

// maybeASAN runs ASAN if the ASAN build tag is enabled.
func maybeASAN() {
	// Aggressively GC so that we can minimize false positives.
	runtime.GC()
	runtime.GC()
	C.__lsan_do_leak_check()
}
