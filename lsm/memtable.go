package lsm

import (
	"github.com/A-walker-ninght/miniKV/utils"
	"github.com/A-walker-ninght/miniKV/wal"
)

type memtable struct {
	s     *utils.Skiplist
	wal   *wal.Wal
	close func()
}

// func NewMemTable() *memtable {

// }
