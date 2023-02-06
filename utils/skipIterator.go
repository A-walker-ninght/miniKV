package utils

import (
	"github.com/A-walker-ninght/miniKV/codec"
)

type SkiplistInterator struct {
	list *Skiplist
	n    *Node
	size int64
}

func (s *SkiplistInterator) Next() {
	if !s.Valid() {
		return
	}
	s.n = s.n.levels[0]
}

// 判断迭代的Node是否为空
func (s *SkiplistInterator) Valid() bool {
	return s.n != nil
}

func (s *SkiplistInterator) First() {
	s.n = s.list.header.levels[0]
}

func (s *SkiplistInterator) Entry() *codec.Entry {
	if !s.Valid() {
		return nil
	}
	return s.n.entry
}

func (s *SkiplistInterator) Seek(key string) {
	if !s.Valid() {
		return
	}
	s.n = s.list.findNode(key)
}
