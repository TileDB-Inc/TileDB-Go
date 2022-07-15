package tiledb

import "runtime"

// Freeable represents an object that can be Free'd at the end of its lifetime
// to release its resources.
type Freeable interface {
	Free() // Releases non–garbage-collected resources held by this object.
}

// freeOnGC sets a finalizer on the provided object that will cause it to
// automatically be Free'd when it is collected by the garbage collecter.
// It should be included immediately after the err-check of the code which
// creates it:
//
//     func NewThingy() (*Thingy, error) {
//       thingy := Thingy{}
//       ret := C.tiledb_make_thingy(&thingy)
//       if ret != C.TILEDB_OK {
//         return nil, errors.New("whatever")
//       }
//       freeOnGC(&thingy)  // <-- put this here
//       return &thingy, nil
//     }
func freeOnGC(obj Freeable) {
	runtime.SetFinalizer(obj, freeFreeable)
}

// freeFreeable frees the Freeable. It's free-floating to avoid capturing
// anything in a closure.
func freeFreeable(obj Freeable) { obj.Free() }
