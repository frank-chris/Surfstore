package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStoreInterface interface {
	// Retrieves the server's FileInfoMap
	GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error)

	// Update a file's fileinfo entry
	UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error)

	// Retrieve the mapping of BlockStore addresses to block hashes
	GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error)

	// Retrieve all BlockStore Addresses
	GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error)
}

type BlockStoreInterface interface {

	// Get a block based on
	GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error)

	// Put a block
	PutBlock(ctx context.Context, block *Block) (*Success, error)

	// Given a list of hashes “in”, returns a list containing the
	// subset of in that are stored in the key-value store
	HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error)

	// Get which blocks are on this BlockStore server
	GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error)
}

type ClientInterface interface {
	// MetaStore
	GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error
	UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error
	GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error
	GetBlockStoreAddrs(blockStoreAddrs *[]string) error

	// BlockStore
	GetBlock(blockHash string, blockStoreAddr string, block *Block) error
	PutBlock(block *Block, blockStoreAddr string, succ *bool) error
	HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error
	GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error
}
