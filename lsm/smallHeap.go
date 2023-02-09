package lsm

import (
	"github.com/A-walker-ninght/miniKV/codec"
)

type heapData struct {
	entry *codec.Entry
	index int // 记录合并的sst索引
}

// 实现一个小根堆
type heap struct {
	data []heapData
	cap  int
}

func newHeap(cap int) *heap {
	return &heap{
		data: make([]heapData, 0),
		cap:  cap,
	}
}

func (h *heap) Less(i, j int) bool { return h.data[i].entry.Key < h.data[j].entry.Key }
func (h *heap) Len() int           { return len(h.data) }
func (h *heap) Swap(i, j int)      { h.data[i], h.data[j] = h.data[j], h.data[i] }

// 小的上移
func (h *heap) up(i int) {
	for {
		fa := (i - 1) / 2
		// 父节点更小
		if fa == i || h.Less(fa, i) {
			return
		}
		h.Swap(i, fa)
		i = fa
	}
}

func (h *heap) down(i int) {
	for {
		l, r := 2*i+1, 2*i+2
		if l >= h.Len() {
			return
		}
		j := l
		// 右节点更小
		if r < h.Len() && !h.Less(l, r) {
			j = r
		}
		// 右孩子更大
		if h.Less(i, j) {
			return
		}
		h.Swap(i, j)
		i = j
	}
}

func (h *heap) Push(data heapData) {
	h.data = append(h.data, data)
	h.up(h.Len() - 1)
}

func (h *heap) Pop() heapData {
	res := h.remove(0)
	return res
}

func (h *heap) remove(i int) heapData {
	if i < 0 || i > h.Len() {
		return heapData{nil, -1}
	}
	n := h.Len() - 1
	h.Swap(i, n) // 替换最后的元素和该元素
	res := h.data[n]
	h.data = h.data[:n]

	if i == 0 || h.Less(i, (i-1)/2) {
		h.down(i)
	} else {
		h.up(i)
	}
	return res
}
