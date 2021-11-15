//go:build asan
// +build asan

package main

// #cgo CFLAGS: -fsanitize=address
// #cgo LDFLAGS: -fsanitize=address
// void __lsan_do_leak_check(void);
import "C"

// maybeASAN runs ASAN if the ASAN build tag is enabled.
func maybeASAN() {
	C.__lsan_do_leak_check()
}
