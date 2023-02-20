package utils

import (
	"math/rand"
	"strings"
	"sync"

	"github.com/A-walker-ninght/miniKV/codec"
)

const (
	maxLevel = 48
)

type Node struct {
	levels []*Node
	entry  *codec.Entry
}

type Skiplist struct {
	header   *Node
	length   int
	capacity int
	lock     *sync.RWMutex
	close    bool
}

func newNode(entry *codec.Entry, level int) *Node {
	return &Node{
		levels: make([]*Node, level),
		entry:  entry,
	}
}

func (n *Node) GetEntry() codec.Entry {
	return *n.entry
}

func NewSkipList() *Skiplist {
	header := &Node{
		levels: make([]*Node, maxLevel),
	}

	return &Skiplist{
		header: header,
		lock:   &sync.RWMutex{},
	}
}

func (s *Skiplist) Add(data *codec.Entry) error {
	head := s.header
	prev := head
	prevs := make([]*Node, maxLevel)

	for i := maxLevel - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = next.levels[i] {
			if comp := strings.Compare(data.Key, next.entry.Key); comp >= 0 {
				if comp == 0 {
					// 更新数据
					if prev.levels[0].entry.Key != data.Key {
						continue
					}
					prev.levels[0].entry = data
					return nil
				} else {
					prev = next
				}
			} else {
				break
			}
		}
		prevs[i] = prev
	}
	level := s.randLevel()

	e := newNode(data, level)
	for i := level - 1; i >= 0; i-- {
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = e
		e.levels[i] = ne
	}
	s.length++
	return nil
}

func (s *Skiplist) Search(key string) (*codec.Entry, codec.Status) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	head := s.header
	prev := head

	for i := maxLevel - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = next.levels[i] {
			if comp := strings.Compare(key, next.entry.Key); comp >= 0 {
				if comp == 0 {
					if prev.levels[0].entry.Key != key {
						continue
					}
					if prev.levels[0].entry.Deleted {
						return nil, codec.Deleted
					}
					return prev.levels[0].entry, codec.Found
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}
	return nil, codec.NotFound
}

func (s *Skiplist) GetCount() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.length
}

func (s *Skiplist) FindNode(key string) *Node {
	s.lock.RLock()
	defer s.lock.RUnlock()

	head := s.header
	prev := head
	for i := maxLevel - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = next.levels[i] {
			if comp := strings.Compare(key, next.entry.Key); comp >= 0 {
				if comp == 0 {
					if prev.levels[0].entry.Key != key {
						continue
					}
					return prev.levels[0]
				} else {
					prev = next
				}
			} else {
				break
			}
		}
	}
	return nil
}

func (s *Skiplist) NewSkiplistInterator() *SkiplistInterator {
	s.lock.Lock()
	defer s.lock.Unlock()
	return &SkiplistInterator{
		list: s,
	}
}

// 获取高度
func (s *Skiplist) randLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < 0.25 {
		level++
	}
	return level
}
