package wal

import (
	"sync"

	"github.com/A-walker-ninght/miniKV/file"
)

type Wal struct {
	f        file.IOSelector
	lock     *sync.RWMutex
	filepath string
}

// // 初始化wal，并加载进内存
// func InitWal(dir string, filesize int64) *utils.Skiplist {
// 	log.Printf("Loading wal.log")
// 	start := time.Now()
// 	defer func() {
// 		end := time.Since(start)
// 		log.Printf("Loading wal.log consume time: %v\n", end)
// 	}()

// 	s := strings.Builder{}
// 	s.WriteString(dir)
// 	s.WriteString(".log")
// 	walPath := s.String()

// 	// fd, err := file.CreateNewWal(walPath, filesize)
// 	// if err != nil {
// 	// 	log.Printf("Open Wal False: %s", &err)
// 	// 	return nil
// 	// }

// }
