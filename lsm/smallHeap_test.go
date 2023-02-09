package lsm

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

type sss []heapData

func (s sss) Len() int           { return len(s) }
func (s sss) Less(i, j int) bool { return s[i].entry.Key < s[j].entry.Key }
func (s sss) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func TestSmallHeap(t *testing.T) {
	data1 := []heapData{}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		entry := codec.NewEntry(key, []byte(key))
		h := heapData{&entry, i}
		data1 = append(data1, h)
	}

	data2 := []heapData{}
	newH := newHeap(1000)
	for i := 999; i >= 0; i-- {
		key := fmt.Sprintf("key%d", i)
		entry := codec.NewEntry(key, []byte(key))
		h := heapData{&entry, i}
		newH.Push(h)

	}

	for newH.Len() > 0 {
		data2 = append(data2, newH.Pop())
	}
	sort.Sort(sss(data1))
	for i := 0; i < 1000; i++ {
		assert.Equal(t, data1[i], data2[i])
	}
}
