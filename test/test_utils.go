package SurfTest

import (
	"bufio"
	"bytes"
	"cse224/proj5/pkg/surfstore"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

const SRC_PATH = "./test_files"
const BLOCK_SIZE = 1024
const META_FILENAME = "index.db"

func IsTombHashList(hashList []string) bool {
	return len(hashList) == 1 && hashList[0] == TOMBSTONE_HASH
}

func SameHashList(list1, list2 []string) bool {
	if len(list1) != len(list2) {
		return false
	}

	for i := 0; i < len(list1); i++ {
		if list1[i] != list2[i] {
			return false
		}
	}

	return true
}

/* SQL statement */

const getDistinctFileName string = `select distinct fileName, version from indexes;`

const getTuplesByFileName string = `select hashIndex, hashValue from indexes where fileName = ? order by hashIndex;`

const createTable string = `create table if not exists indexes (
	fileName TEXT, 
	version INT,
	hashIndex INT,
	hashValue TEXT
);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES(?,?,?,?);`

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

func LoadMetaFromDB(baseDir string) (fileMetaMap map[string]*surfstore.FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*surfstore.FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	// stmt, _ := db.Prepare(createTable)
	// _, err = stmt.Exec()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	row_fn, _ := db.Query(getDistinctFileName)
	var fileName string
	var version int
	for row_fn.Next() {
		var blockHashList []string
		row_fn.Scan(&fileName, &version)
		row_hash, _ := db.Query(getTuplesByFileName, fileName)
		for row_hash.Next() {
			var hashIndex int
			var hashValue string
			row_hash.Scan(&hashIndex, &hashValue)
			blockHashList = append(blockHashList, hashValue)
		}
		fileMetaMap[fileName] = &surfstore.FileMetaData{
			Filename:      fileName,
			Version:       int32(version),
			BlockHashList: blockHashList,
		}
	}
	return fileMetaMap, nil
}

func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*surfstore.FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))

	fileMetaMap = make(map[string]*surfstore.FileMetaData)

	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	metaFD, e := os.Open(metaFilePath)
	if e != nil {
		return nil, e
	}
	defer metaFD.Close()

	leftOverContent := ""
	metaReader := bufio.NewReader(metaFD)
	for {
		lineContent, isPrefix, e := metaReader.ReadLine()
		if e != nil && e != io.EOF {
			return nil, e
		}

		leftOverContent += string(lineContent)
		if isPrefix {
			continue
		}

		if len(leftOverContent) == 0 {
			break
		}

		currFileMeta := NewFileMetaData(META_INIT_BY_CONFIG_STR,
			"", 0, nil, leftOverContent)

		leftOverContent = ""
		fileMetaMap[currFileMeta.Filename] = currFileMeta
	}

	return fileMetaMap, nil
}

func CreateDir(dirPath string) {
	_ = os.Mkdir(dirPath, os.FileMode(0755))
}

func CleanUpDir(dirPath string) {
	_ = os.RemoveAll(dirPath)
}

func AppendFile(filename, message string) error {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)

	if err != nil {
		fmt.Println(err)
		return err
	}
	defer f.Close()

	_, _ = fmt.Fprintf(f, "%s\n", message)
	return nil
}

func TruncateFile(filename string, leftSize int) error {
	return os.Truncate(filename, int64(leftSize))
}

func DeleteFile(filename string) error {
	return os.Remove(filename)
}

func CopyFile(sourceFile, destinationFile string) error {
	input, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		fmt.Println(err)
		return err
	}

	emptyFile, _ := os.Create(destinationFile)
	defer emptyFile.Close()
	err = ioutil.WriteFile(destinationFile, input, 0644)
	if err != nil {
		fmt.Println("Error creating", destinationFile)
		fmt.Println(err)
		return err
	}
	return nil
}

func DirFullySynced(worker1, worker2 DirectoryWorker) bool {
	fileMap1 := worker1.ListAllFile()
	fileMap2 := worker2.ListAllFile()

	for filename1 := range fileMap1 {
		_, exist := fileMap2[filename1]
		if !exist {
			return false
		}
	}

	for filename2 := range fileMap2 {
		_, exist := fileMap1[filename2]
		if !exist {
			return false
		}
	}

	for filename := range fileMap1 {
		if filepath.Base(filename) == DEFAULT_META_FILENAME {
			continue
		}

		c, _ := SameFile(worker1.DirectoryName+"/"+filename, worker2.DirectoryName+"/"+filename)
		if !c {
			return false
		}
	}

	return true
}

func SameFile(filename1, filename2 string) (bool, error) {
	f1, err1 := ioutil.ReadFile(filename1)

	if err1 != nil {
		return false, err1
	}

	f2, err2 := ioutil.ReadFile(filename2)

	if err2 != nil {
		return false, err2
	}

	return bytes.Equal(f1, f2), nil
}

func SameMeta(meta1, meta2 map[string]*surfstore.FileMetaData) bool {

	for filename1, filemeta1 := range meta1 {
		filemeta2, exist := meta2[filename1]
		if !exist ||
			filemeta2.Version != filemeta1.Version ||
			!SameHashList(filemeta2.BlockHashList, filemeta1.BlockHashList) {
			return false
		}
	}

	for filename2, filemeta2 := range meta2 {
		filemeta1, exist := meta1[filename2]
		if !exist ||
			filemeta2.Version != filemeta1.Version ||
			!SameHashList(filemeta2.BlockHashList, filemeta1.BlockHashList) {
			return false
		}
	}

	return true
}

func InitSurfServers(blockStores int) []*exec.Cmd {
	cmdList := make([]*exec.Cmd, 0)
	if blockStores == 0 {
		serverCmd := exec.Command("_bin/SurfstoreServerExec", "-s", "both", "-l", "localhost:8080")
		serverCmd.Stderr = os.Stderr
		serverCmd.Stdout = os.Stdout
		cmdList = append(cmdList, serverCmd)
	} else {
		metaArgs := []string{"-s", "meta", "-l"}
		for i := 1; i <= blockStores; i++ {
			port := 8080 + i
			blockCmd := exec.Command("_bin/SurfstoreServerExec", "-s", "block", "-p", strconv.Itoa(port), "-l")
			blockCmd.Stderr = os.Stderr
			blockCmd.Stdout = os.Stdout
			cmdList = append(cmdList, blockCmd)

			metaArgs = append(metaArgs, "localhost:"+strconv.Itoa(port))
		}

		metaCmd := exec.Command("_bin/SurfstoreServerExec", metaArgs...)
		metaCmd.Stderr = os.Stderr
		metaCmd.Stdout = os.Stdout
		cmdList = append(cmdList, metaCmd)
	}

	return cmdList
}

func StartSurfServers(servers []*exec.Cmd, ready chan bool) {
	for _, server := range servers {
		err := server.Start()
		if err != nil {
			log.Println(err)
		}
	}

	ready <- true
}

func KillSurfServers(servers []*exec.Cmd) {
	for _, server := range servers {
		_ = server.Process.Kill()
	}

	exec.Command("pkill SurfstoreServerExec*")
}

func SyncClient(metaAddr, baseDir string, blockSize int, cfgPath string) error {
	clientCmd := exec.Command("_bin/SurfstoreClientExec", "-f", cfgPath, baseDir, strconv.Itoa(blockSize))
	clientCmd.Stderr = os.Stderr
	clientCmd.Stdout = os.Stdout

	return clientCmd.Run()
}

func assert(cond bool) {
	if !cond {
		debug.PrintStack()
		log.Fatal("assertion failed")
	}
}

func noError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
