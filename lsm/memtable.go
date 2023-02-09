package lsm

import (
	"strings"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/utils"
)

type Memtable struct {
	s         *utils.Skiplist
	wal       *Wal
	threshold int  // 插入的数据个数
	id        int  // 区分wal, ./wal_id.go
	convert   bool // 区分memtable和immumemtable, false: memtable
	lock      *sync.RWMutex
}

func NewMemTable(threshold int) *Memtable {
	id := int(time.Now().Unix())

	return &Memtable{
		wal:       &Wal{},
		threshold: threshold,
		id:        id,
		lock:      &sync.RWMutex{},
	}
}

// 初始化, Memtable
func (m *Memtable) InitMemTable(filepath string) {
	s := strings.Split(filepath, ".")
	sl := m.wal.InitWal(1000)
	if s[len(s)-1] == "iog" {
		m.convert = true
	}
	m.s = sl
}

func (m *Memtable) Convert() bool {

	m.convert = true
	m.wal.ReName(m.id)
	return true
}
