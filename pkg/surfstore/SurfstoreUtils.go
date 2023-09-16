package surfstore

import (
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
)

// return errors TODO
func upload(client RPCClient, metadata *FileMetaData) error {
	filePath := client.BaseDir + "/" + metadata.Filename
	var latestVersion int32 //int

	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		err = client.UpdateFile(metadata, &latestVersion)
		if err != nil {
			log.Println("Error updating file: ", err)
			log.Panic()
		}
		metadata.Version = latestVersion
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Println("Error getting file info: ", err)
	}

	var numBlocks int = int(math.Ceil(float64(fileInfo.Size()) / float64(client.BlockSize)))

	for i := 0; i < numBlocks; i++ {
		block := make([]byte, client.BlockSize)
		n, err := file.Read(block)
		if err != nil && err != io.EOF {
			log.Println("Error reading file: ", err)
		}
		block = block[:n]
		blockStruct := Block{BlockData: block, BlockSize: int32(n)}

		blockHash := GetBlockHashString(block)
		var blockStoreMap map[string][]string
		if err := client.GetBlockStoreMap([]string{blockHash}, &blockStoreMap); err != nil {
			log.Println("Error getting block store map: ", err)
			log.Panic()
		}
		reverseMap := make(map[string]string)
		for key, val := range blockStoreMap {
			for _, address := range val {
				reverseMap[address] = key
			}
		}

		var success bool
		blockStoreAddress := reverseMap[blockHash]
		err = client.PutBlock(&blockStruct, blockStoreAddress, &success)
		if err != nil {
			log.Println("Error putting block: ", err)
		}
	}

	err = client.UpdateFile(metadata, &latestVersion)
	if err != nil {
		log.Println("Error updating file: ", err)
		metadata.Version = -1
		log.Panic()
	}
	metadata.Version = latestVersion
	return nil
}

func download(client RPCClient, remoteMetadata *FileMetaData, localMetadata *FileMetaData) error {
	filePath := client.BaseDir + "/" + remoteMetadata.Filename
	file, err := os.Create(filePath)
	if err != nil {
		log.Println("Error creating file: ", err)
	}
	defer file.Close()

	*localMetadata = *remoteMetadata

	if len(remoteMetadata.BlockHashList) == 1 && remoteMetadata.BlockHashList[0] == "0" {
		err := os.Remove(filePath)
		if err != nil {
			log.Println("Error removing file: ", err)
			return err
		}
		return nil
	}

	data := ""

	var blockStoreMap map[string][]string

	if err := client.GetBlockStoreMap(remoteMetadata.BlockHashList, &blockStoreMap); err != nil {
		log.Println("Error getting block store map: ", err)
		log.Panic()
	}

	reverseMap := make(map[string]string)

	for key, val := range blockStoreMap {
		for _, address := range val {
			reverseMap[address] = key
		}
	}

	for _, hash := range remoteMetadata.BlockHashList {
		var block Block
		blockStoreAddress := reverseMap[hash]
		err := client.GetBlock(hash, blockStoreAddress, &block)
		if err != nil {
			log.Println("Error getting block: ", err)
		}
		data += string(block.BlockData)
	}
	file.WriteString(data)
	return nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	indexFilePath := client.BaseDir + "/index.db"
	_, err := os.Stat(indexFilePath)
	if os.IsNotExist(err) {
		indexFile, _ := os.Create(indexFilePath)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// defer indexFile.Close()
		indexFile.Close()
	}

	entries, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error reading BaseDir: ", err)
	}

	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error loading local metadata: ", err)
	}

	hashMap := make(map[string][]string)

	for _, entry := range entries {
		if entry.Name() == "index.db" {
			continue
		}

		var numBlocks int = int(math.Ceil(float64(entry.Size()) / float64(client.BlockSize)))
		readFile, err := os.Open(client.BaseDir + "/" + entry.Name())
		if err != nil {
			log.Println("Error opening file: ", err)
		}
		defer readFile.Close()

		for i := 0; i < numBlocks; i++ {
			block := make([]byte, client.BlockSize)
			n, err := readFile.Read(block)
			if err != nil {
				log.Println("Error reading file: ", err)
			}
			block = block[:n]
			hash := GetBlockHashString(block)
			hashMap[entry.Name()] = append(hashMap[entry.Name()], hash)
		}

		if val, ok := localMetaMap[entry.Name()]; ok {
			if !reflect.DeepEqual(val.BlockHashList, hashMap[entry.Name()]) {
				localMetaMap[entry.Name()].BlockHashList = hashMap[entry.Name()]
				localMetaMap[entry.Name()].Version++
			}
		} else {
			meta := FileMetaData{Filename: entry.Name(), Version: 1, BlockHashList: hashMap[entry.Name()]}
			localMetaMap[entry.Name()] = &meta
		}
	}

	for filename, metadata := range localMetaMap {
		if _, ok := hashMap[filename]; !ok {
			if len(metadata.BlockHashList) != 1 || metadata.BlockHashList[0] != "0" {
				metadata.BlockHashList = []string{"0"}
				metadata.Version++
			}
		}
	}

	// var blockStoreAddress string

	// if err := client.GetBlockStoreAddr(&blockStoreAddress); err != nil {
	// 	log.Println("Error getting block store address: ", err)
	// }

	remoteMetaMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&remoteMetaMap); err != nil {
		log.Println("Error getting remote metadata: ", err)
		log.Panic()
	}

	for filename, metadata := range localMetaMap {
		if val, ok := remoteMetaMap[filename]; ok {
			if metadata.Version > val.Version {
				upload(client, metadata)
			}
		} else {
			upload(client, metadata)
		}
	}

	for filename, metadata := range remoteMetaMap {
		if localMetadata, ok := localMetaMap[filename]; ok {
			if metadata.Version > localMetadata.Version {
				download(client, metadata, localMetadata)
			} else if metadata.Version == localMetadata.Version && !reflect.DeepEqual(metadata.BlockHashList, localMetadata.BlockHashList) {
				download(client, metadata, localMetadata)
			}
		} else {
			localMetaMap[filename] = &FileMetaData{}
			localMetadata := localMetaMap[filename]
			download(client, metadata, localMetadata)
		}
	}
	WriteMetaFile(localMetaMap, client.BaseDir)
}
