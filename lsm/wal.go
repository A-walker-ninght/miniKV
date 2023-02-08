package lsm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/file"
	"github.com/A-walker-ninght/miniKV/utils"
)

// |dataLen data | dataLen data | dataLen data |
// dataLen : int64
type Wal struct {
	f        file.IOSelector
	lock     *sync.RWMutex
	filepath string
	p        int64 // 文件指针
}

// 从磁盘读取，初始化Wal
func (w *Wal) InitWal(filesize int64) *utils.Skiplist {
	log.Printf("Loading wal.log")
	start := time.Now()
	dir := config.CheckConfig().WalDir
	defer func() {
		end := time.Since(start)
		log.Printf("Loading wal.log consume time: %v\n", end)
	}()

	s := strings.Builder{}
	s.WriteString(dir)
	s.WriteString("wal.log")
	walPath := s.String()

	// 获取信息
	info, err := os.Stat(w.filepath)
	if err != nil {
		fmt.Errorf("Wal filePath InValid: %s", w.filepath)
	}

	// info为空，创建wal
	if info == nil {
		fd, err := file.OpenMMapFile(walPath, filesize)
		if err != nil {
			fmt.Errorf("Open Wal False: %s", err)
			return nil
		}
		w.f = fd
		w.filepath = walPath
		w.lock = &sync.RWMutex{}
		return w.recovery(true)
	}

	size := info.Size()

	if size > filesize {
		filesize = size
	}
	fd, err := file.OpenMMapFile(walPath, filesize)
	if err != nil {
		fmt.Errorf("Open Wal False: %s", err)
		return nil
	}

	w.f = fd
	w.filepath = walPath
	w.lock = &sync.RWMutex{}

	return w.recovery(false)
}

func (w *Wal) recovery(isCreate bool) *utils.Skiplist {
	w.lock.Lock()
	defer w.lock.Unlock()
	var sl *utils.Skiplist
	sl = utils.NewSkipList()
	if isCreate {
		return sl
	}

	var dataLen int64
	// 文件指针
	var p int64
	// var e *codec.Entry 妈的，卡了好久
	for {
		var e *codec.Entry
		dataLenBuf := make([]byte, 8)
		n, err := w.f.(*file.MMapFile).Read(dataLenBuf, p)
		if err != nil {
			fmt.Errorf("dataLen Read False: %s", err)
		}

		p += 8
		dataLen = int64(binary.BigEndian.Uint64(dataLenBuf))

		data := make([]byte, dataLen)
		n, err = w.f.(*file.MMapFile).Read(data, p)
		if n == 0 {
			break
		}
		if err != nil {
			fmt.Errorf("data Read False: %s", err)
		}
		err = json.Unmarshal(data, &e)
		if err != nil {
			fmt.Errorf("data Unmarshal False: %s", err)
		}
		if e.Deleted {
			sl.Delete(e.Key)
		} else {
			sl.Add(e)
		}
		p += int64(n)
	}
	w.p = p
	return sl
}

func (w *Wal) Write(e *codec.Entry) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	data, err := json.Marshal(e)
	if err != nil {
		fmt.Errorf("Wal Write False: %s", err)
		return err
	}
	dataLenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(dataLenBuf, uint64(len(data)))
	n, err := w.f.(*file.MMapFile).Write(dataLenBuf, w.p)
	if err != nil {
		fmt.Errorf("Wal dataLen Write False: %s", err)
		return err
	}
	w.p += int64(n)

	n, err = w.f.(*file.MMapFile).Write(data, w.p)
	if err != nil {
		fmt.Errorf("Wal data Write False: %s", err)
		return err
	}
	w.p += int64(n)
	w.f.(*file.MMapFile).Sync() // 每次写入都刷盘
	return nil
}

func (w *Wal) Reset() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	err := w.f.(*file.MMapFile).Delete()
	if err != nil {
		fmt.Errorf("Wal Reset False: %s", err)
	}
	return err
}

func (w *Wal) ReName(id int) {
	w.f.(*file.MMapFile).Close()

	filepaths := strings.Split(w.filepath, "/")
	s := strings.Builder{}
	s.WriteString("wal_")
	s.WriteString(strconv.Itoa(id))
	s.WriteString(".iog")
	filepaths[len(filepaths)-1] = s.String()
	path := strings.Join(filepaths, "/")

	err := os.Rename(w.filepath, path)
	if err != nil {
		fmt.Printf("wal rename false")
	}
	w.filepath = path
}
