package examples_lib

import (
	"os"
)

// checkError panics is err is not nil
func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

// checkedValue is a wrapper for methods returning (any, error).
// The intented usage is v := checkedValue(method()). It returns
// the value of method() unless an error happens then it panics.
func checkedValue[T any](v T, err error) T {
	checkError(err)
	return v
}

// temp creates a temporary directory which contains the given name,
// or panics if it cannot.
func temp(name string) string {
	dir, err := os.MkdirTemp("", name)
	checkError(err)
	return dir
}

// cleanup os.RemoveAlls the given path, or panics if it cannot.
func cleanup(name string) {
	checkError(os.RemoveAll(name))
}
