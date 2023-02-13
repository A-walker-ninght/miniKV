package lsm

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/utils"
)

type Memtable struct {
	s         *utils.Skiplist
	wal       *Wal
	threshold int  // 插入的数据个数阈值
	convert   bool // 区分memtable和immumemtable, false: memtable
	lock      *sync.RWMutex
}

func NewMemTable(threshold int, filepath string) *Memtable {
	m := &Memtable{
		wal:       &Wal{},
		threshold: threshold,
		lock:      &sync.RWMutex{},
	}
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

func (m *Memtable) Search(key string) []byte {
	e, f := m.s.Search(key)

	if !f {
		return []byte{}
	}
	return e.Value
}

func (m *Memtable) Delete(data codec.Entry) error {
	data.Deleted = true
	err := m.wal.Write(&data)
	if err != nil {
		return err
	}

	err = m.s.Add(&data)
	if err != nil {
		return err
	}
	return nil
}

func (m *Memtable) Add(data codec.Entry) error {
	e := codec.NewEntry(data.Key, data.Value, data.Version)
	e.Deleted = false

	err := m.wal.Write(&e)
	if err != nil {
		return err
	}
	err = m.s.Add(&e)
	if err != nil {
		return err
	}
	return nil
}

func (m *Memtable) getAll() (data []codec.Entry) {
	iter := m.s.NewSkiplistInterator()
	for iter.First(); iter.Valid(); iter.Next() {
		data = append(data, *iter.Entry())
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
	s.WriteString("../logFile/wal/wal_")
	s.WriteString(strconv.Itoa(int(id)))
	s.WriteString(".iog")
	filepath := s.String()
	newM := NewMemTable(m.threshold, filepath)
	data := m.getAll()
	for _, e := range data {
		newM.Add(e)
	}
	m.wal.Reset()
	return newM
}
