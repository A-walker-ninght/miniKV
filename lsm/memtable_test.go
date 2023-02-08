package lsm

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/stretchr/testify/assert"

	"sync"
	"testing"
)

func TestMemtableBasic(t *testing.T) {
	// 创建
	wg := sync.WaitGroup{}
	m := NewMemTable(10000)
	m.InitMemTable("./")
	entrys := []codec.Entry{}

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := RandString(10)
			e := codec.NewEntry(key, []byte(RandString(10)))
			err, _ := m.Add(&e)
			if err != nil {
				fmt.Println("Add False")
				return
			}
			entrys = append(entrys, e)
		}()
	}
	wg.Wait()

	for _, e := range entrys {
		el, _ := m.Search(e.Key)
		assert.Equal(t, e, *el)
	}

}
