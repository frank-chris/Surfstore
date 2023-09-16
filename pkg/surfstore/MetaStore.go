package surfstore

import (
	context "context"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	mtx                sync.Mutex
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	filename := fileMetaData.Filename
	version := fileMetaData.Version
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if _, ok := m.FileMetaMap[filename]; ok {
		if version == m.FileMetaMap[filename].Version+1 {
			m.FileMetaMap[filename] = fileMetaData
		} else {
			version = -1
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
	}
	return &Version{Version: version}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := BlockStoreMap{BlockStoreMap: map[string]*BlockHashes{}}
	for _, blockHash := range blockHashesIn.Hashes {
		// check if blockHash is in the map
		serverAddress := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap.BlockStoreMap[serverAddress]; ok {
			blockStoreMap.BlockStoreMap[serverAddress].Hashes = append(blockStoreMap.BlockStoreMap[serverAddress].Hashes, blockHash)
		} else {
			blockStoreMap.BlockStoreMap[serverAddress] = &BlockHashes{Hashes: []string{blockHash}}
		}
	}
	return &blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
