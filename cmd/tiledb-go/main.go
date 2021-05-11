package main

// void __lsan_do_leak_check(void);
//
// import "C"
import "github.com/TileDB-Inc/TileDB-Go/examples_lib"

func main() {
	examples_lib.RunArrayMetadataArray()
	examples_lib.RunAsyncArray()
	examples_lib.RunConfig()
	examples_lib.RunDeserializeSparseLayouts()
	examples_lib.RunEncryptedArray()
	examples_lib.RunErrors()
	examples_lib.RunFiltersArray()
	examples_lib.RunFragmentsConsolidationArray()
	examples_lib.RunMultiAttributeArray()
	examples_lib.RunDenseArray()
	examples_lib.RunSparseArray()
	examples_lib.RunWritingSparseMultiple()
	examples_lib.RunRange()
	examples_lib.RunReadingDenseLayouts()
	examples_lib.RunReadingIncompleteArray()
	examples_lib.RunReadRangeArray()
	examples_lib.RunReadingSparseLayouts()
	examples_lib.RunTimestampArray()
	examples_lib.RunStringDimArray()
	examples_lib.RunUsingTileDBStats()
	examples_lib.RunVacuumSparseArray()
	examples_lib.RunVariableLengthArray()
	examples_lib.RunVfs()
	examples_lib.RunWritingDenseGlobalExpansion()
	examples_lib.RunWritingDenseGlobal()
	examples_lib.RunWritingDenseMultiple()
	examples_lib.RunWritingDensePadding()
	examples_lib.RunWritingDenseSparse()
	examples_lib.RunWritingSparseGlobal()
	examples_lib.RunSparseHeterDimArray()
	examples_lib.RunWritingSparseMultiple()

	C.__lsan_do_leak_check()
}
