package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"strconv"
	"strings"
)

func NewFileMetaData(InitMode int, filename string, version int, hashList []string, configStr string) *surfstore.FileMetaData {
	switch InitMode {
	case META_INIT_BY_PARAMS:
		return NewFileMetaDataFromParams(filename, version, hashList)
	case META_INIT_BY_CONFIG_STR:
		return NewFileMetaDataFromConfig(configStr)
	}

	// In default case, we return an empty file metadata object
	return &surfstore.FileMetaData{}
}

func NewFileMetaDataFromConfig(configString string) *surfstore.FileMetaData {
	configItems := strings.Split(configString, CONFIG_DELIMITER)

	filename := configItems[FILENAME_INDEX]
	version, _ := strconv.Atoi(configItems[VERSION_INDEX])

	blockHashList := strings.Split(strings.TrimSpace(configItems[HASH_LIST_INDEX]), HASH_DELIMITER)

	return &surfstore.FileMetaData{
		Filename:      filename,
		Version:       int32(version),
		BlockHashList: blockHashList,
	}
}

func NewFileMetaDataFromParams(filename string, version int, hashList []string) *surfstore.FileMetaData {
	return &surfstore.FileMetaData{
		Filename:      filename,
		Version:       int32(version),
		BlockHashList: hashList,
	}
}
