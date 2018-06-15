package tiledb

// Compressor represents a tiledb compression method
type Compressor struct {
	Compressor CompressorType
	Level      int
}

func (c *Compressor) Str() string {
	switch c.Compressor {
	case TILEDB_NO_COMPRESSION:
		return "NO_COMPRESSION"
	case TILEDB_GZIP:
		return "GZIP"
	case TILEDB_ZSTD:
		return "ZSTD"
	case TILEDB_LZ4:
		return "LZ4"
	case TILEDB_BLOSC_LZ:
		return "BLOSC_LZ"
	case TILEDB_BLOSC_LZ4:
		return "BLOSC_LZ4"
	case TILEDB_BLOSC_LZ4HC:
		return "BLOSC_LZ4HC"
	case TILEDB_BLOSC_SNAPPY:
		return "BLOSC_SNAPPY"
	case TILEDB_BLOSC_ZLIB:
		return "BLOSC_ZLIB"
	case TILEDB_BLOSC_ZSTD:
		return "BLOSC_ZSTD"
	case TILEDB_RLE:
		return "RLE"
	case TILEDB_BZIP2:
		return "BZIP2"
	case TILEDB_DOUBLE_DELTA:
		return "DOUBLE_DELTA"
	}
	return "Invalid"
}
