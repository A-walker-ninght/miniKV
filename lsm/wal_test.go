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
	s := w.InitWal(10000, "wal.log")

	keys := []string{}
	for i := 0; i < 10000; i++ {
		key, val = fmt.Sprintf("Key%d", i), fmt.Sprintf("Val%d", i)
		entry := codec.NewEntry(key, []byte(val))
		w.Write(entry)
		res := s.Add(&entry)

		keys = append(keys, key)
		assert.Equal(t, res, nil)
		searchVal, _ := s.Search(key)
		assert.Equal(t, searchVal.Value, []byte(val))
	}
	w.f.(*file.MMapFile).Sync()

	// 恢复recovery
	newSl := w.InitWal(1000, "../logFile/wal/wal.log")
	for _, key := range keys {
		n1, _ := newSl.Search(key)
		n2, _ := s.Search(key)
		assert.Equal(t, n1, n2)
	}

	w.Reset()
	info, _ := os.Stat("../logFile/wal/wal.log")
	assert.Nil(t, info)
}
