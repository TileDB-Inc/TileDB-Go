package examples_lib

import (
	"io/ioutil"
	"os"
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func checkedValue[T any](v T, err error) T {
	checkError(err)
	return v
}

// temp creates a temporary directory which contains the given name,
// or panics if it cannot.
func temp(name string) string {
	dir, err := ioutil.TempDir("", name)
	checkError(err)
	return dir
}

// cleanup os.RemoveAlls the given path, or panics if it cannot.
func cleanup(name string) {
	checkError(os.RemoveAll(name))
}
