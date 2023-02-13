package lsm

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	// 初始化opt
	levelSize = config.LevelSize{
		LSizes: []int{2, 8, 16, 32, 64, 128, 256},
	}
	opt = config.Config{
		WalDir:        "../logFile/wal/",
		DataDir:       "../logFile/sst/",
		PartSize:      10,
		Threshold:     1000,
		CheckInterval: 1 * time.Microsecond,
		MaxLevelNum:   7,
		LevelSize:     levelSize,
	}
)

func TestLSMAdd(t *testing.T) {
	lsm := NewLSM(opt)
	entrys := []codec.Entry{}
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		e := codec.NewEntry(key, value, int64(i))
		entrys = append(entrys, e)
		assert.Nil(t, lsm.Set(key, value, int64(i)))
	}

	for i := 0; i < 10000; i++ {
		value := lsm.Search(entrys[i].Key)
		assert.Equal(t, value, entrys[i].Value)
	}
}

func Benchmark_LSMAdd(b *testing.B) {
	lsm := NewLSM(opt)
	entrys := []codec.Entry{}
	for i := 0; i < b.N; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		e := codec.NewEntry(key, value, int64(i))
		entrys = append(entrys, e)
		go assert.Nil(b, lsm.Set(key, value, int64(i)))
	}

	for i := 0; i < b.N; i++ {
		value := lsm.Search(entrys[i].Key)
		assert.Equal(b, value, entrys[i].Value)
	}
}

func TestLSMDelete(t *testing.T) {
	lsm := NewLSM(opt)

	entrys := []codec.Entry{}
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		e := codec.NewEntry(key, value, int64(i))
		entrys = append(entrys, e)
		assert.Nil(t, lsm.Set(key, value, int64(i)))
	}

	for i := 0; i < 10000; i++ {
		value := lsm.Search(entrys[i].Key)
		assert.Equal(t, value, entrys[i].Value)
	}

	for i := 0; i < 232; i++ {
		lsm.Delete(entrys[i].Key, int64(i+10000))
	}

	for i := 0; i < 232; i++ {
		ss := lsm.Search(entrys[i].Key)
		fmt.Println(ss)
		assert.Equal(t, ss, []byte{})
	}
}