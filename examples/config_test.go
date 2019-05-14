/**
 * @file   config_test.go
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This is a part of the TileDB tutorial:
 *   https://docs.tiledb.io/en/latest/tutorials/config.html
 *
 * This program shows how to set/get the TileDB configuration parameters.
 */

package examples

import (
	"fmt"
	"os"

	"github.com/TileDB-Inc/TileDB-Go"
)

var configFileName = "tiledb_config.txt"

func setGetConfigCtxVfs() {
	// Create config objects
	config, err := tiledb.NewConfig()

	// Set/Get config to/from ctx
	ctx, err := tiledb.NewContext(config)
	checkError(err)
	configCtx, err := ctx.Config()
	checkError(err)
	err = configCtx.Set("param1", "value1")
	checkError(err)

	vfs, err := tiledb.NewVFS(ctx, config)
	checkError(err)
	configVfs, err := vfs.Config()
	checkError(err)
	err = configVfs.Set("param2", "value2")
	checkError(err)

	param1, err := configCtx.Get("param1")
	checkError(err)
	fmt.Println(param1)
	param2, err := configVfs.Get("param2")
	checkError(err)
	fmt.Println(param2)
}

func setGetConfig() {
	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)

	// Set a value
	err = config.Set("vfs.s3.connect_timeout_ms", "5000")
	checkError(err)

	// Get the value and print it
	vfsConnectTimeoutMs, err := config.Get("vfs.s3.connect_timeout_ms")
	fmt.Printf("VFS connect timeout in ms is: %s\n", vfsConnectTimeoutMs)

	// Get another value and print it
	tileCacheSize, err := config.Get("sm.tile_cache_size")
	fmt.Printf("Tile cache size: %s\n", tileCacheSize)
}

func printDefault() {
	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)

	// Get default settings and print them
	fmt.Println("Default settings:")
	smArraySchemaCacheSize, err := config.Get("sm.array_schema_cache_size")
	checkError(err)
	fmt.Printf("\"sm.array_schema_cache_size\" : \"%s\"\n",
		smArraySchemaCacheSize)

	smCheckCoordDups, err := config.Get("sm.check_coord_dups")
	checkError(err)
	fmt.Printf("\"sm.check_coord_dups\" : \"%s\"\n",
		smCheckCoordDups)

	smCheckCoordOob, err := config.Get("sm.check_coord_oob")
	checkError(err)
	fmt.Printf("\"sm.check_coord_oob\" : \"%s\"\n",
		smCheckCoordOob)

	smCheckGlobalOrder, err := config.Get("sm.check_global_order")
	checkError(err)
	fmt.Printf("\"sm.check_global_order\" : \"%s\"\n",
		smCheckGlobalOrder)

	smConsolidationAmplification, err := config.Get("sm.consolidation." +
		"amplification")
	checkError(err)
	fmt.Printf("\"sm.consolidation.amplification\" : \"%s\"\n",
		smConsolidationAmplification)

	smConsolidationBufferSize, err := config.Get("sm.consolidation." +
		"buffer_size")
	checkError(err)
	fmt.Printf("\"sm.consolidation.buffer_size\" : \"%s\"\n",
		smConsolidationBufferSize)

	smConsolidationStepMaxFrags, err := config.Get("sm.consolidation." +
		"step_max_frags")
	checkError(err)
	fmt.Printf("\"sm.consolidation.step_max_frags\" : \"%s\"\n",
		smConsolidationStepMaxFrags)

	smConsolidationStepMinFrags, err := config.Get("sm.consolidation." +
		"step_min_frags")
	checkError(err)
	fmt.Printf("\"sm.consolidation.step_min_frags\" : \"%s\"\n",
		smConsolidationStepMinFrags)

	smConsolidationStepSizeRatio, err := config.Get("sm.consolidation." +
		"step_size_ratio")
	checkError(err)
	fmt.Printf("\"sm.consolidation.step_size_ratio\" : \"%s\"\n",
		smConsolidationStepSizeRatio)

	smConsolidationSteps, err := config.Get("sm.consolidation.steps")
	checkError(err)
	fmt.Printf("\"sm.consolidation.steps\" : \"%s\"\n",
		smConsolidationSteps)

	smDedupCoords, err := config.Get("sm.dedup_coords")
	checkError(err)
	fmt.Printf("\"sm.dedup_coords\" : \"%s\"\n",
		smDedupCoords)

	smEnableSignalHandlers, err := config.Get("sm.enable_signal_handlers")
	checkError(err)
	fmt.Printf("\"sm.enable_signal_handlers\" : \"%s\"\n",
		smEnableSignalHandlers)

	smFragmentMetadataCacheSize, err := config.Get("sm." +
		"fragment_metadata_cache_size")
	checkError(err)
	fmt.Printf("\"sm.fragment_metadata_cache_size\" : \"%s\"\n",
		smFragmentMetadataCacheSize)

	smNumAsyncThreads, err := config.Get("sm.num_async_threads")
	checkError(err)
	fmt.Printf("\"sm.num_async_threads\" : \"%s\"\n",
		smNumAsyncThreads)

	smNumReaderThreads, err := config.Get("sm.num_reader_threads")
	checkError(err)
	fmt.Printf("\"sm.num_reader_threads\" : \"%s\"\n",
		smNumReaderThreads)

	smNumTbbThreads, err := config.Get("sm.num_tbb_threads")
	checkError(err)
	fmt.Printf("\"sm.num_tbb_threads\" : \"%s\"\n",
		smNumTbbThreads)

	smNumWriterThreads, err := config.Get("sm.num_writer_threads")
	checkError(err)
	fmt.Printf("\"sm.num_writer_threads\" : \"%s\"\n",
		smNumWriterThreads)

	smTileCacheSize, err := config.Get("sm.tile_cache_size")
	checkError(err)
	fmt.Printf("\"sm.tile_cache_size\" : \"%s\"\n",
		smTileCacheSize)

	vfsFileMaxParallelOps, err := config.Get("vfs.file.max_parallel_ops")
	checkError(err)
	fmt.Printf("\"vfs.file.max_parallel_ops\" : \"%s\"\n",
		vfsFileMaxParallelOps)

	vfsHdfsKerbTicketCachePath, err := config.Get("vfs.hdfs." +
		"kerb_ticket_cache_path")
	checkError(err)
	fmt.Printf("\"vfs.hdfs.kerb_ticket_cache_path\" : \"%s\"\n",
		vfsHdfsKerbTicketCachePath)

	vfsHdfsNameNodeUri, err := config.Get("vfs.hdfs.name_node_uri")
	checkError(err)
	fmt.Printf("\"vfs.hdfs.name_node_uri\" : \"%s\"\n",
		vfsHdfsNameNodeUri)

	vfsHdfsUsername, err := config.Get("vfs.hdfs.username")
	checkError(err)
	fmt.Printf("\"vfs.hdfs.username\" : \"%s\"\n",
		vfsHdfsUsername)

	vfsMaxBatchReadAmplification, err := config.Get("vfs." +
		"max_batch_read_amplification")
	checkError(err)
	fmt.Printf("\"vfs.max_batch_read_amplification\" : \"%s\"\n",
		vfsMaxBatchReadAmplification)

	vfsMaxBatchReadSize, err := config.Get("vfs.max_batch_read_size")
	checkError(err)
	fmt.Printf("\"vfs.max_batch_read_size\" : \"%s\"\n",
		vfsMaxBatchReadSize)

	vfsMinParallelSize, err := config.Get("vfs.min_parallel_size")
	checkError(err)
	fmt.Printf("\"vfs.min_parallel_size\" : \"%s\"\n",
		vfsMinParallelSize)

	vfsNumThreads, err := config.Get("vfs.num_threads")
	checkError(err)
	fmt.Printf("\"vfs.num_threads\" : \"%s\"\n",
		vfsNumThreads)

	vfsS3AwsAccessKeyId, err := config.Get("vfs.s3.aws_access_key_id")
	checkError(err)
	fmt.Printf("\"vfs.s3.aws_access_key_id\" : \"%s\"\n",
		vfsS3AwsAccessKeyId)

	vfsS3AwsSecretAccessKey, err := config.Get("vfs.s3." +
		"aws_secret_access_key")
	checkError(err)
	fmt.Printf("\"vfs.s3.aws_secret_access_key\" : \"%s\"\n",
		vfsS3AwsSecretAccessKey)

	vfsS3ConnectMaxTries, err := config.Get("vfs.s3.connect_max_tries")
	checkError(err)
	fmt.Printf("\"vfs.s3.connect_max_tries\" : \"%s\"\n",
		vfsS3ConnectMaxTries)

	vfsS3ConnectScaleFactor, err := config.Get("vfs.s3.connect_scale_factor")
	checkError(err)
	fmt.Printf("\"vfs.s3.connect_scale_factor\" : \"%s\"\n",
		vfsS3ConnectScaleFactor)

	vfsS3ConnectTimeoutMs, err := config.Get("vfs.s3.connect_timeout_ms")
	checkError(err)
	fmt.Printf("\"vfs.s3.connect_timeout_ms\" : \"%s\"\n",
		vfsS3ConnectTimeoutMs)

	vfsS3EndpointOverride, err := config.Get("vfs.s3.endpoint_override")
	checkError(err)
	fmt.Printf("\"vfs.s3.endpoint_override\" : \"%s\"\n",
		vfsS3EndpointOverride)

	vfsS3MaxParallelOps, err := config.Get("vfs.s3.max_parallel_ops")
	checkError(err)
	fmt.Printf("\"vfs.s3.max_parallel_ops\" : \"%s\"\n",
		vfsS3MaxParallelOps)

	vfsS3MultipartPartSize, err := config.Get("vfs.s3.multipart_part_size")
	checkError(err)
	fmt.Printf("\"vfs.s3.multipart_part_size\" : \"%s\"\n",
		vfsS3MultipartPartSize)

	vfsS3ProxyHost, err := config.Get("vfs.s3.proxy_host")
	checkError(err)
	fmt.Printf("\"vfs.s3.proxy_host\" : \"%s\"\n",
		vfsS3ProxyHost)

	vfsS3ProxyPassword, err := config.Get("vfs.s3.proxy_password")
	checkError(err)
	fmt.Printf("\"vfs.s3.proxy_password\" : \"%s\"\n",
		vfsS3ProxyPassword)

	vfsS3ProxyPort, err := config.Get("vfs.s3.proxy_port")
	checkError(err)
	fmt.Printf("\"vfs.s3.proxy_port\" : \"%s\"\n",
		vfsS3ProxyPort)

	vfsS3ProxyScheme, err := config.Get("vfs.s3.proxy_scheme")
	checkError(err)
	fmt.Printf("\"vfs.s3.proxy_scheme\" : \"%s\"\n",
		vfsS3ProxyScheme)

	vfsS3ProxyUsername, err := config.Get("vfs.s3.proxy_username")
	checkError(err)
	fmt.Printf("\"vfs.s3.proxy_username\" : \"%s\"\n",
		vfsS3ProxyUsername)

	vfsS3Region, err := config.Get("vfs.s3.region")
	checkError(err)
	fmt.Printf("\"vfs.s3.region\" : \"%s\"\n",
		vfsS3Region)

	vfsS3RequestTimeoutMs, err := config.Get("vfs.s3.request_timeout_ms")
	checkError(err)
	fmt.Printf("\"vfs.s3.request_timeout_ms\" : \"%s\"\n",
		vfsS3RequestTimeoutMs)

	vfsS3Scheme, err := config.Get("vfs.s3.scheme")
	checkError(err)
	fmt.Printf("\"vfs.s3.scheme\" : \"%s\"\n",
		vfsS3Scheme)

	vfsS3UseVirtualAddressing, err := config.Get("vfs.s3." +
		"use_virtual_addressing")
	checkError(err)
	fmt.Printf("\"vfs.s3.use_virtual_addressing\" : \"%s\"\n",
		vfsS3UseVirtualAddressing)
}

func saveLoadConfig() {
	fmt.Println("Save and load config")

	// Create config object
	config, err := tiledb.NewConfig()
	checkError(err)

	// Set a value
	err = config.Set("sm.tile_cache_size", "8")
	checkError(err)

	// Save to disk
	err = config.SaveToFile(configFileName)
	checkError(err)

	// Load config from file
	newConfig, err := tiledb.LoadConfig(configFileName)
	checkError(err)

	// Print the retrieved value
	smTileCacheSize, err := newConfig.Get("sm.tile_cache_size")
	checkError(err)
	fmt.Printf("\"sm.tile_cache_size\" : \"%s\"\n",
		smTileCacheSize)

	// Clean up
	err = os.RemoveAll(configFileName)
	checkError(err)
}

func ExampleConfig() {
	setGetConfigCtxVfs()
	setGetConfig()
	printDefault()
	saveLoadConfig()
}
