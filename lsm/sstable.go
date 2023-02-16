package lsm

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
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
	p        int64           // 文件指针
	idxArea  IdxArea         // 索引区
	size     int64
	lock     *sync.RWMutex
	meta     MetaInfo
	maxKey   string
	minKey   string
}

type IdxArea struct {
	Pos  map[string]Position // key: Position
	Keys []string            // 按key大小排序
	Door *utils.BloomFilter  // 通过布隆过滤器快速判断key是否在sst
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

func OpenSSTable(filepath string) (*SSTable, error) {
	info, _ := os.Stat(filepath)
	if info == nil {
		return nil, errors.New("The SSTable file is not exist!")
	}
	fd, err := file.OpenMMapFile(filepath, info.Size())

	if err != nil {
		return nil, errors.New("Create SSTable False!")
	}

	sst := &SSTable{
		f:        fd,
		filePath: filepath,
		lock:     &sync.RWMutex{},
		size:     info.Size(),
	}
	sst.openSSTable()
	return sst, nil
}

func (sst *SSTable) openSSTable() {
	metaBuf := make([]byte, 40)
	sst.f.(*file.MMapFile).Read(metaBuf, sst.size-40)
	sst.meta.dataStart = int64(binary.BigEndian.Uint64(metaBuf[:8]))
	sst.meta.dataLen = int64(binary.BigEndian.Uint64(metaBuf[8:16]))
	sst.meta.idxStart = int64(binary.BigEndian.Uint64(metaBuf[16:24]))
	sst.meta.idxLen = int64(binary.BigEndian.Uint64(metaBuf[24:32]))
	sst.meta.version = int64(binary.BigEndian.Uint64(metaBuf[32:40]))

	// 索引区
	idxArea := make([]byte, sst.meta.idxLen)
	sst.f.(*file.MMapFile).Read(idxArea, sst.meta.idxStart)

	var idx IdxArea
	err := json.Unmarshal(idxArea, &idx)
	if err != nil {
		fmt.Errorf("OpenSSTable idxArea Unmarshal False: %s", err)
		return
	}
	sst.idxArea = idx
	sst.minKey = sst.idxArea.Keys[0]
	sst.maxKey = sst.idxArea.Keys[len(sst.idxArea.Keys)-1]
}

// 创建sst文件，写入磁盘，同时保存结构体
func CreateNewSSTable(data []codec.Entry, filepath string, size int64) (*SSTable, error) {
	fd, err := file.OpenMMapFile(filepath, size)
	if err != nil {
		return nil, errors.New("Create SSTable False!")
	}
	sst := &SSTable{
		f:        fd,
		filePath: filepath,
		lock:     &sync.RWMutex{},
	}
	sst.initSST(data)
	return sst, nil
}

func (sst *SSTable) initSST(data []codec.Entry) {
	if len(data) == 0 {
		return
	}
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
		Pos:  poss,
		Door: door,
		Keys: keys,
	}
	sst.minKey = idxArea.Keys[0]
	sst.maxKey = idxArea.Keys[len(idxArea.Keys)-1]
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
	meta.idxLen = int64(n)

	sst.meta = meta
	// meta
	metaBuf := make([]byte, 40)
	binary.BigEndian.PutUint64(metaBuf[:8], uint64(meta.dataStart))
	binary.BigEndian.PutUint64(metaBuf[8:16], uint64(meta.dataLen))
	binary.BigEndian.PutUint64(metaBuf[16:24], uint64(meta.idxStart))
	binary.BigEndian.PutUint64(metaBuf[24:32], uint64(meta.idxLen))
	binary.BigEndian.PutUint64(metaBuf[32:40], uint64(meta.version))
	_, err = sst.f.(*file.MMapFile).Write(metaBuf, sst.p)

	if err != nil {
		log.Printf("MetaInfo Write Buffer False: %s", err)
		return
	}
	sst.f.(*file.MMapFile).Truncature(sst.p + 40)
	// 写入磁盘
	err = sst.f.(*file.MMapFile).Sync()
	if err != nil {
		log.Printf("Buffer Write To File False: %s", err)
	}
	sst.size = sst.f.(*file.MMapFile).Size()
}

func (sst *SSTable) Remove() error {
	if sst == nil {
		return errors.New("sst file is not exist!")
	}
	return sst.f.(*file.MMapFile).Delete()
}

func (sst *SSTable) Size() int64 {
	if sst == nil {
		return 0
	}
	return sst.f.(*file.MMapFile).Size()
}
