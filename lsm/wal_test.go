package lsm

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/file"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWalBasic(t *testing.T) {
	key, val := "", ""
	w := &Wal{}
	s := w.InitWal(10000)

	keys := []string{}
	for i := 0; i < 1000; i++ {
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry(key, []byte(val))
		w.Write(&entry)
		res := s.Add(&entry)

		keys = append(keys, key)
		assert.Equal(t, res, nil)
		searchVal := s.Search(key)
		assert.Equal(t, searchVal.Value, []byte(val))
	}
	w.f.(*file.MMapFile).Sync()

	// 恢复recovery
	newSl := w.InitWal(1000)
	for _, key := range keys {
		assert.Equal(t, newSl.Search(key), s.Search(key))
	}

	w.Reset()
	info, _ := os.Stat("./wal.log")
	assert.Nil(t, info)
}
