package lsm

import (
	"encoding/json"
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/file"
	"github.com/A-walker-ninght/miniKV/utils"
	"github.com/stretchr/testify/assert"
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

func TestSSTableBasic(t *testing.T) {
	list := utils.NewSkipList()
	wg := sync.WaitGroup{}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			entry := codec.NewEntry(RandString(10), []byte(RandString(10)))
			assert.Nil(t, list.Add(&entry))
			wg.Done()
		}()
	}
	wg.Wait()

	iter := list.NewSkiplistInterator()
	entrys := []*codec.Entry{}
	for iter.First(); iter.Valid(); iter.Next() {
		entrys = append(entrys, iter.Entry())
	}
	sst, err := CreateNewSSTable(entrys, "./sst.txt", int64(100000000))
	if err != nil {
		t.Errorf("OpenSSTable False!")
	}
	fmt.Printf("filepath: %v\n, idxArea: %v\n, lock: %v\n, p: %v\n, meta: %v\n",
		sst.filePath, sst.idxArea, sst.lock, sst.p, sst.meta)
	buf := make([]byte, 10)
	n, _ := sst.f.(*file.MMapFile).Read(buf, 100000)
	var idx IdxArea
	json.Unmarshal(buf[:n], &idx)
	fmt.Printf("idxArea: %+v", sst.idxArea)
}
