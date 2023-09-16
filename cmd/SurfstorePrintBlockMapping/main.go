package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

// Arguments
const ARG_COUNT int = 2

// Usage strings
const USAGE_STRING = "./run-client.sh -d -f config_file.txt baseDir blockSize"

const DEBUG_NAME = "d"
const DEBUG_USAGE = "Output log statements"

const CONFIG_NAME = "f config_file.txt"
const CONFIG_USAGE = "Path to config file that specifies addresses for all Raft nodes"

const BASEDIR_NAME = "baseDir"
const BASEDIR_USAGE = "Base directory of the client"

const BLOCK_NAME = "blockSize"
const BLOCK_USAGE = "Size of the blocks used to fragment files"

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		fmt.Fprintf(w, "  -%s: %v\n", DEBUG_NAME, DEBUG_USAGE)
		fmt.Fprintf(w, "  -%s: %v\n", CONFIG_NAME, CONFIG_USAGE)
		fmt.Fprintf(w, "  %s: %v\n", BASEDIR_NAME, BASEDIR_USAGE)
		fmt.Fprintf(w, "  %s: %v\n", BLOCK_NAME, BLOCK_USAGE)
	}

	// Parse command-line arguments and flags
	debug := flag.Bool("d", false, DEBUG_USAGE)
	configFile := flag.String("f", "", "(required) Config file")
	flag.Parse()

	// Use tail arguments to hold non-flag arguments
	args := flag.Args()

	if len(args) != ARG_COUNT {
		flag.Usage()
		os.Exit(EX_USAGE)
	}
	addrs := surfstore.LoadRaftConfigFile(*configFile)

	baseDir := args[0]
	blockSize, err := strconv.Atoi(args[1])
	if err != nil {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	rpcClient := surfstore.NewSurfstoreRPCClient(addrs.RaftAddrs, baseDir, blockSize)
	PrintBlocksOnEachServer(rpcClient)
}

func PrintBlocksOnEachServer(client surfstore.RPCClient) {
	allAddrs := []string{}
	err := client.GetBlockStoreAddrs(&allAddrs)
	if err != nil {
		log.Fatal("[Surfstore RPCClient]:", "Error During Fetching All BlockStore Addresses ", err)
	}

	result := "{"
	for _, addr := range allAddrs {
		// fmt.Println("Block Server: ", addr)
		hashes := []string{}
		if err = client.GetBlockHashes(addr, &hashes); err != nil {
			log.Fatal("[Surfstore RPCClient]:", "Error During Fetching Blocks on Block Server ", err)
		}

		for _, hash := range hashes {
			result += "{" + hash + "," + addr + "},"
		}
	}
	if len(result) == 1 {
		result = "{}"
	} else {
		result = result[:len(result)-1] + "}"
	}
	fmt.Println(result)
}
