package cache

import (
	"container/list"
	"unsafe"
)

type WindowLRU struct {
	data map[uint32]*list.Element
	cap  int
	list *list.List
}

type Item struct {
	key   uint32
	stage int
	value []byte
}

func newWindowLRU(cap int, data map[uint32]*list.Element) *WindowLRU {
	return &WindowLRU{
		data: data,
		cap:  cap,
		list: list.New(),
	}
}

// Add，头插，容量满了，淘汰最后一个
func (w *WindowLRU) Put(newItem Item) (vic *Item, vicBool bool) {
	if len(w.data) < w.cap {
		w.data[newItem.key] = w.list.PushFront(&newItem)
		return &Item{}, false
	}
	backData := w.list.Back()
	node := backData.Value.(*Item)

	delete(w.data, node.key)
	w.list.Remove(backData)
	w.data[newItem.key] = w.list.PushFront(&newItem)
	return node, true
}

func (w *WindowLRU) Get(e *list.Element) {
	w.list.MoveToFront(e)
} 
