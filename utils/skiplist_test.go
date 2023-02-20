package utils

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sync"
	"testing"
)

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListBasicCRUD(t *testing.T) {
	list := NewSkipList()

	// 插入和查找
	entry1 := codec.NewEntry("key1", []byte("Val1"))
	assert.Nil(t, list.Add(&entry1))
	v, _ := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, v.Value)

	entry2 := codec.NewEntry("key2", []byte("Val2"))
	assert.Nil(t, list.Add(&entry2))
	v, _ = list.Search(entry2.Key)
	assert.Equal(t, entry2.Value, v.Value)

	v, _ = list.Search(entry1.Key)
	assert.Equal(t, entry1.Deleted, v.Deleted)
	// 查找不存在的key
	v, _ = list.Search("notExist")

	assert.Nil(t, v)

	// 更新
	entry2_new := codec.NewEntry("key1", []byte("Val1+1"))
	assert.Nil(t, list.Add(&entry2_new))
	v, _ = list.Search(entry2_new.Key)
	assert.Equal(t, entry2_new.Value, v.Value)
}

func Benchmark_SkipListBasicCRUD(b *testing.B) {
	list := NewSkipList()
	key, val := "", ""

	for i := 0; i < 1000000; i++ {
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry(key, []byte(val))
		res := list.Add(&entry)
		assert.Equal(b, res, nil)
		searchVal, _ := list.Search(key)
		assert.Equal(b, searchVal.Value, []byte(val))

	}
}

func TestConcurrentBasic(t *testing.T) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key, value := fmt.Sprintf("%05d", i), fmt.Sprintf("%05d", i)
			entry := codec.NewEntry(key, []byte(value))
			assert.Nil(t, l.Add(&entry))
		}(i)
	}
	wg.Wait()

	// 并发读
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%05d", i)
			v, _ := l.Search(key)
			if v != nil {
				require.EqualValues(t, key, v.Value)
				return
			}
			require.Nil(t, v)
		}(i)
	}
	wg.Wait()
}

func Benchmark_ConcurrentBasic(b *testing.B) {
	const n = 1000
	l := NewSkipList()
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key, val := fmt.Sprintf("%05d", i), fmt.Sprintf("%05d", i)
			entry := codec.NewEntry(key, []byte(val))
			assert.Nil(b, l.Add(&entry))
		}(i)
	}
	wg.Wait()

	// Check values. Concurrent reads.
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := fmt.Sprintf("%05d", i)
			v, _ := l.Search(k)
			if v != nil {
				require.EqualValues(b, k, v.Value)
				return
			}
			require.Nil(b, v)
		}(i)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	list := NewSkipList()

	//Put & Get
	entry1 := codec.NewEntry(RandString(10), []byte(RandString(10)))
	list.Add(&entry1)
	v, _ := list.Search(entry1.Key)
	assert.Equal(t, entry1.Value, v.Value)

	entry2 := codec.NewEntry(RandString(10), []byte(RandString(10)))
	list.Add(&entry2)
	v, _ = list.Search(entry2.Key)

	assert.Equal(t, entry2.Value, v.Value)

	//Update a entry
	entry2_new := codec.NewEntry(entry2.Key, []byte(RandString(10)))
	list.Add(&entry2_new)
	v, _ = list.Search(entry2_new.Key)
	assert.Equal(t, entry2_new.Value, v.Value)

	iter := list.NewSkiplistInterator()
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Printf("iter key %s, value %s", iter.Entry().Key, iter.Entry().Value)
	}
}

func Benchmark_SkipListIterator(b *testing.B) {
	wg := sync.WaitGroup{}
	list := NewSkipList()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			entry := codec.NewEntry(RandString(10), []byte(RandString(10)))
			assert.Nil(b, list.Add(&entry))
			wg.Done()
		}(i)
	}
	wg.Wait()

	iter := list.NewSkiplistInterator()
	entrys := []*codec.Entry{}
	for iter.First(); iter.Valid(); iter.Next() {
		entrys = append(entrys, iter.Entry())
		fmt.Printf("key: %s, value: %s\n", iter.Entry().Key, iter.Entry().Value)
	}
	for i := 1; i < len(entrys); i++ {
		assert.True(b, entrys[i-1].Key <= entrys[i].Key)
	}
}

func TestDelete(t *testing.T) {
	list := NewSkipList()
	key, val := "", ""

	for i := 0; i < 100000; i++ {
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry(key, []byte(val))
		res := list.Add(&entry)
		assert.Equal(t, res, nil)
		searchVal, _ := list.Search(key)
		assert.Equal(t, searchVal.Value, []byte(val))

	}

	for i := 0; i < 1000; i++ {
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry(key, []byte(val))
		entry.Deleted = true
		res := list.Add(&entry)
		assert.Equal(t, res, nil)
		_, f := list.Search(key)
		assert.Equal(t, f, codec.Deleted)
	}
}
