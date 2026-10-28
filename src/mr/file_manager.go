package mr

import (
	"log"
	"sync"
)

type FileManager struct {
	availableFiles map[string]struct{}
	runningFiles   map[string]struct{}
	doneFiles      map[string]struct{}
	filesMutex     sync.Mutex
}

func NewFileManager() *FileManager {
	return &FileManager{
		availableFiles: make(map[string]struct{}),
		runningFiles:   make(map[string]struct{}),
		doneFiles:      make(map[string]struct{}),
	}
}

func (fm *FileManager) LoadFiles(files []string) {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	for _, file := range files {
		fm.availableFiles[file] = struct{}{}
	}
}

func (fm *FileManager) AddFile(file string) {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	if _, ok := fm.doneFiles[file]; ok {
		log.Printf("Attempt to add done file %s\n", file)
	}
	if _, ok := fm.runningFiles[file]; ok {
		log.Printf("Attempt to add running file %s\n", file)
	}
	delete(fm.doneFiles, file)
	delete(fm.runningFiles, file)
}

func (fm *FileManager) FileDone(file string) {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	if _, ok := fm.availableFiles[file]; ok {
		log.Printf("Attempt to mark available file done %s\n", file)
	}
	delete(fm.availableFiles, file)
	delete(fm.runningFiles, file)
}

func (fm *FileManager) Poll() (string, error) {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	if len(fm.availableFiles) <= 0 {
		return "", ErrNoAvailabeFiles
	}
	var file string
	for f := range fm.availableFiles {
		file = f
		break
	}

	delete(fm.availableFiles, file)
	delete(fm.doneFiles, file)

	fm.runningFiles[file] = struct{}{}
	return file, nil
}

func (fm *FileManager) FileAvailableLen() int {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	return len(fm.availableFiles)
}

func (fm *FileManager) FileDoneLen() int {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	return len(fm.doneFiles)
}

func (fm *FileManager) IsAllFilesDone() bool {
	fm.filesMutex.Lock()
	defer fm.filesMutex.Unlock()
	return len(fm.availableFiles) == 0 && len(fm.runningFiles) == 0
}
