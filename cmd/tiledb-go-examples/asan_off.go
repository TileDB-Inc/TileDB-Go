// +build !asan

package main

// maybeASAN runs ASAN if the ASAN build tag is enabled.
func maybeASAN() { /* don't run ASAN. */ }
