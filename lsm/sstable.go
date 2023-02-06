package lsm

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/file"
	"github.com/A-walker-ninght/miniKV/utils"
)

// |————————————||————————————||——————————————|
// |            ||            ||              |
// |            ||            ||              |
// |  data area || index area ||   meta area  |
// |            ||            ||              |
// |————————————||————————————||——————————————|

// meta area
// |dataStart|dataLen|idxStart|idxLen|version|
// SSTable 表，存储在磁盘文件中
type SSTable struct {
	f        file.IOSelector // 文件句柄
	filePath string          // 路径
	idxArea  IdxArea         // 索引区
	lock     *sync.RWMutex
	p        int64 // 文件指针
	meta     MetaInfo
}

type IdxArea struct {
	pos  map[string]Position // key: Position
	keys []string            // 按key大小排序
	door *utils.BloomFilter  // 通过布隆过滤器快速判断key是否在sst
}

type MetaInfo struct {
	version   int64
	dataStart int64
	dataLen   int64
	idxStart  int64
	idxLen    int64
}

type Position struct {
	Offset  int64 // 起始索引
	Len     int   // 长度
	Deleted bool  // Key 已经被删除
}

// 创建sst文件，写入磁盘，同时保存结构体
func CreateNewSSTable(data []*codec.Entry, filepath string, size int64) (*SSTable, error) {
	fd, err := file.NewMMapFile(filepath, size)
	if err != nil {
		return nil, errors.New("Create SSTable False!")
	}
	sst := &SSTable{
		f:        fd,
		filePath: filepath,
		lock:     &sync.RWMutex{},
	}
	sst.InitSST(data)
	return sst, nil
}

func (sst *SSTable) InitSST(data []*codec.Entry) {
	keys := make([]string, 0)
	poss := make(map[string]Position, 0)
	door := utils.NewFilter(len(data), 0.01)

	for _, e := range data {
		keys = append(keys, e.Key)
		pos := Position{
			Offset:  sst.p,
			Len:     len(e.Value),
			Deleted: e.Deleted,
		}
		poss[e.Key] = pos
		if !door.Insert(e.Key) {
			log.Printf("BloomFilter Insert False!")
			continue
		}

		n, err := sst.f.(*file.MMapFile).Write(e.Value, sst.p) // 写入buf
		if err != nil {
			log.Printf("Value Write Buffer False: %s", err)
			continue
		}
		sst.p += int64(n) // 移动指针
	}

	meta := MetaInfo{
		dataStart: 0,
		dataLen:   sst.p,
		idxStart:  sst.p,
	}

	// idxArea
	idxArea := IdxArea{
		pos:  poss,
		door: door,
		keys: keys,
	}
	sst.idxArea = idxArea
	idx, err := json.Marshal(idxArea)
	if err != nil {
		log.Printf("idxArea Marshal False: %s", err)
		return
	}
	n, err := sst.f.(*file.MMapFile).Write(idx, sst.p)
	if err != nil {
		log.Printf("idxArea Write Buffer False: %s", err)
		return
	}
	sst.p += int64(n)
	meta.idxLen = sst.p
	meta.version = 0

	sst.meta = meta
	// meta
	metaBuf := make([]byte, 40)
	l := 0
	l += binary.PutVarint(metaBuf, meta.dataStart)
	l += binary.PutVarint(metaBuf, meta.dataLen)
	l += binary.PutVarint(metaBuf, meta.idxStart)
	l += binary.PutVarint(metaBuf, meta.idxLen)
	l += binary.PutVarint(metaBuf, meta.version)
	_, err = sst.f.(*file.MMapFile).Write(metaBuf[:l], sst.p)

	if err != nil {
		log.Printf("MetaInfo Write Buffer False: %s", err)
		return
	}
	// 写入磁盘
	err = sst.f.(*file.MMapFile).Sync()
	if err != nil {
		log.Printf("Buffer Write To File False: %s", err)
	}
}
