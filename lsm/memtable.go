package lsm

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/tools"
	"github.com/A-walker-ninght/miniKV/utils"
)

type Memtable struct {
	s         *utils.Skiplist
	wal       *Wal
	threshold int  // 插入的数据个数阈值
	convert   bool // 区分memtable和immumemtable, false: memtable
	lock      *sync.RWMutex
}

func NewMemTable(fileName string) *Memtable {
	config := config.GetConfig()
	m := &Memtable{
		wal:       &Wal{},
		threshold: config.Threshold,
		lock:      &sync.RWMutex{},
	}
	filepath := tools.GetFilePath(config.WalDir, fileName)
	m.initMemTable(filepath)
	return m
}

// 初始化, Memtable
func (m *Memtable) initMemTable(filepath string) {
	s := strings.Split(filepath, ".")
	sl := m.wal.InitWal(1000, filepath)
	if s[len(s)-1] == "iog" {
		m.convert = true
	}
	m.s = sl
}

func (m *Memtable) Search(key string) ([]byte, codec.Status) {
	e, f := m.s.Search(key)

	if f == codec.Deleted {
		return []byte{}, codec.Deleted
	}
	if f == codec.NotFound {
		return []byte{}, codec.NotFound
	}
	return e.Value, codec.Found
}

func (m *Memtable) Delete(data *codec.Entry) error {
	data.Deleted = true
	err := m.wal.Write(*data)
	if err != nil {
		return err
	}

	err = m.s.Add(data)
	if err != nil {
		return err
	}
	return nil
}

func (m *Memtable) Add(data *codec.Entry) error {
	err := m.wal.Write(*data)
	if err != nil {
		return err
	}
	err = m.s.Add(data)
	if err != nil {
		return err
	}
	return nil
}

func (m *Memtable) getAll() (data []*codec.Entry) {
	iter := m.s.NewSkiplistInterator()
	for iter.First(); iter.Valid(); iter.Next() {
		data = append(data, iter.Entry())
	}
	return
}

func (m *Memtable) Convert() *Memtable {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.convert {
		return nil
	}
	if m.s.GetCount() < m.threshold {
		return nil
	}
	id := time.Now().Unix()
	s := strings.Builder{}
	s.WriteString(strconv.Itoa(int(id)))
	s.WriteString(".iog")

	fileName := s.String()
	newM := NewMemTable(fileName)
	data := m.getAll()
	for _, e := range data {
		newM.Add(e)
	}
	m.wal.Reset()
	return newM
}
