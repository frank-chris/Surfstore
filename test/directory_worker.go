package SurfTest

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type DirectoryWorker struct {
	DirectoryName string
	SrcPath       string
}

func InitDirectoryWorker(directoryName, srcPath string) *DirectoryWorker {
	dir, _ := os.Getwd()
	directoryName, _ = filepath.Abs(dir + "/" + directoryName)
	CleanUpDir(directoryName)
	CreateDir(directoryName)
	return &DirectoryWorker{
		DirectoryName: directoryName,
		SrcPath:       srcPath,
	}
}

func (d *DirectoryWorker) CleanUp() {
	CleanUpDir(d.DirectoryName)
}

func (d *DirectoryWorker) ListAllFile() map[string]bool {
	fileMap := make(map[string]bool)

	localFiles, _ := ioutil.ReadDir(d.DirectoryName)

	for _, localFile := range localFiles {
		fileMap[localFile.Name()] = true
	}

	return fileMap
}

func (d *DirectoryWorker) AddFile(filename string) error {
	return CopyFile(d.SrcPath+"/"+filename, d.DirectoryName+"/"+filename)
}

func (d *DirectoryWorker) DeleteFile(filename string) error {
	return DeleteFile(d.DirectoryName + "/" + filename)
}

func (d *DirectoryWorker) TruncateFile(filename string, size int) error {
	return TruncateFile(d.DirectoryName+"/"+filename, size)
}

func (d *DirectoryWorker) UpdateFile(filename, message string) error {
	return AppendFile(d.DirectoryName+"/"+filename, message)
}
