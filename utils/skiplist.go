package utils

import (
	"bytes"
	"math/rand"
	"sync"

	"github.com/A-walker-ninght/miniKV/codec"
)

const (
	maxLevel = 48
)

type Node struct {
	levels []*Node
	entry  *codec.Entry
	score  float64
}

type Skiplist struct {
	header   *Node
	length   int
	capacity int
	lock     sync.RWMutex
	size     int64
}

func newNode(score float64, entry *codec.Entry, level int) *Node {
	return &Node{
		levels: make([]*Node, level),
		entry:  entry,
		score:  score,
	}
}

func NewSkipList() *Skiplist {
	header := &Node{
		levels: make([]*Node, maxLevel),
	}

	return &Skiplist{
		header: header,
	}
}

func (s *Skiplist) Add(data *codec.Entry) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	key := data.Key
	keyScore := s.calcScore(key)
	head := s.header
	prev := head
	prevs := make([]*Node, maxLevel)
	for i := maxLevel - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = next.levels[i] {
			if comp := s.compare(keyScore, key, next); comp <= 0 {
				if comp == 0 {
					// 更新数据
					next.entry = data
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
	level, keyScore := s.randLevel(), s.calcScore(key)
	e := newNode(keyScore, data, level)
	for i := level - 1; i >= 0; i-- {
		ne := prevs[i].levels[i]
		prevs[i].levels[i] = e
		e.levels[i] = ne
	}
	s.length++
	return nil
}

func (s *Skiplist) Search(key []byte) *codec.Entry {
	s.lock.RLock()
	defer s.lock.RUnlock()
	keyScore := s.calcScore(key)
	head := s.header
	prev := head

	for i := maxLevel - 1; i >= 0; i-- {
		for next := prev.levels[i]; next != nil; next = next.levels[i] {
			if comp := s.compare(keyScore, key, next); comp <= 0 {
				if comp == 0 {
					return next.entry
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

// 获取高度
func (s *Skiplist) randLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < 0.25 {
		level++
	}
	return level
}

func (s *Skiplist) compare(score float64, key []byte, next *Node) int {
	if score == next.score {
		return bytes.Compare(key, next.entry.Key)

	}
	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (s *Skiplist) calcScore(key []byte) float64 {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score := float64(hash)
	return score
}
