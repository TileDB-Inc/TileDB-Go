package tiledb

/*
#include <tiledb/tiledb.h>
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"unsafe"
)

// StatsEnable enables internal statistics gathering.
func StatsEnable() error {
	ret := C.tiledb_stats_enable()
	if ret != C.TILEDB_OK {
		return errors.New("error enabling stats")
	}
	return nil
}

// StatsDisable disables internal statistics gathering.
func StatsDisable() error {
	ret := C.tiledb_stats_disable()
	if ret != C.TILEDB_OK {
		return errors.New("error disabling stats")
	}
	return nil
}

// StatsReset resets all internal statistics counters to 0.
func StatsReset() error {
	ret := C.tiledb_stats_reset()
	if ret != C.TILEDB_OK {
		return errors.New("error resetting stats")
	}
	return nil
}

// StatsDumpSTDOUT prints internal stats to stdout.
func StatsDumpSTDOUT() error {
	ret := C.tiledb_stats_dump(C.stdout)
	if ret != C.TILEDB_OK {
		return errors.New("error dumping stats to stdout")
	}
	return nil
}

// StatsDump prints internal stats to the given file path.
func StatsDump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}

	// Convert to char *
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Set mode as char*
	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	// Open file to get FILE*
	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	// Dump stats to file
	ret := C.tiledb_stats_dump(cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping stats to file %s", path)
	}
	return nil
}

// Stats returns internal stats as string.
func Stats() (string, error) {
	var msg *C.char

	// Dump stats to string
	ret := C.tiledb_stats_dump_str(&msg)
	if ret != C.TILEDB_OK {
		return "", errors.New("error dumping stats to string")
	}
	s := C.GoString(msg)

	ret = C.tiledb_stats_free_str(&msg)
	if ret != C.TILEDB_OK {
		return "", errors.New("error freeing string from dumping stats to string")
	}

	return s, nil
}

// StatsRawDumpSTDOUT prints internal raw (json) stats to stdout.
func StatsRawDumpSTDOUT() error {
	ret := C.tiledb_stats_raw_dump(C.stdout)
	if ret != C.TILEDB_OK {
		return errors.New("error dumping stats to stdout")
	}
	return nil
}

// StatsRawDump prints internal raw (json) stats to the given file path.
func StatsRawDump(path string) error {

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("error path already %s exists", path)
	}

	// Convert to char *
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	// Set mode as char*
	cMode := C.CString("w")
	defer C.free(unsafe.Pointer(cMode))

	// Open file to get FILE*
	cFile := C.fopen(cPath, cMode)
	defer C.fclose(cFile)

	// Dump stats to file
	ret := C.tiledb_stats_raw_dump(cFile)
	if ret != C.TILEDB_OK {
		return fmt.Errorf("error dumping stats to file %s", path)
	}
	return nil
}

// StatsRaw returns internal raw (json) stats as string.
func StatsRaw() (string, error) {
	var msg *C.char

	// Dump stats to string
	ret := C.tiledb_stats_raw_dump_str(&msg)
	if ret != C.TILEDB_OK {
		return "", errors.New("error dumping raw stats to string")
	}
	s := C.GoString(msg)

	ret = C.tiledb_stats_free_str(&msg)
	if ret != C.TILEDB_OK {
		return "", errors.New("error freeing string from dumping raw stats to string")
	}

	return s, nil
}

// StatsIsEnabled returns whether stats are enabled or not
func StatsIsEnabled() (bool, error) {
	var isEnabled C.uint8_t
	ret := C.tiledb_stats_is_enabled(&isEnabled)
	if ret != C.TILEDB_OK {
		return false, errors.New("error checking if stats is enabled")
	}
	return isEnabled > 0, nil
}
