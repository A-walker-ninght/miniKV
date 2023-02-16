package lsm

import (
	"fmt"
	"sync"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/file"
)

type levelManager struct {
	levelfile *levelFile
	levels    []*level
	lock      *sync.RWMutex
	levelSize config.LevelSize
}

type level struct {
	Sstable    []*SSTable
	LevelCount int
}

func NewLevelManager(lvFilePath string, levelNum int, lSizes config.LevelSize) *levelManager {
	lm := &levelManager{
		levels:    make([]*level, levelNum),
		lock:      &sync.RWMutex{},
		levelSize: lSizes,
	}

	lm.levelfile = NewlevelFile(levelNum)
	for i := 0; i < levelNum; i++ {
		lm.levels[i] = InitLevel(i, lm.levelfile.levelsfile[i].SSTablePaths)
	}
	return lm
}

func InitLevel(lv int, sstPaths []string) *level {
	l := &level{}
	for i := 0; i < len(sstPaths); i++ {
		sst, err := OpenSSTable(sstPaths[i])
		if err != nil {
			fmt.Errorf("Levels InitLevel OpenSSTable False: %s", err)
			return l
		}
		l.Sstable = append(l.Sstable, sst)
	}
	return l
}

func (l *level) LevelSize() int64 {
	size := int64(0)
	for i := 0; i < len(l.Sstable); i++ {
		size += l.Sstable[i].Size()
	}
	return size
}
func (l *level) search(key string) ([]byte, codec.Status) {

	// 对每一层都进行二分查找，需要从后往前找，因为是追加的
	for i := len(l.Sstable) - 1; i >= 0; i-- {
		// 判断key是否在sst的[min, max]之间
		sst := l.Sstable[i]
		if key < sst.minKey || key > sst.maxKey {
			continue
		}

		// 布隆过滤器过滤key
		if !sst.idxArea.Door.Check(key) {
			continue
		}

		// 通过[]key二分查找
		var position = Position{
			Offset: -1,
		}

		left, right := 0, len(sst.idxArea.Keys)-1
		for left <= right {
			mid := left + (right-left)/2
			if sst.idxArea.Keys[mid] == key {
				position = sst.idxArea.Pos[key]
				if position.Deleted {
					return []byte{}, codec.Deleted
				}
			} else if sst.idxArea.Keys[mid] > key {
				right = mid - 1
			} else if sst.idxArea.Keys[mid] < key {
				left = mid + 1
			}
		}

		// 没找到找下一个sst
		if position.Offset == -1 {
			continue
		}

		// 找到了
		value := make([]byte, position.Len)
		_, err := sst.f.(*file.MMapFile).Read(value, position.Offset)
		if err != nil {
			fmt.Errorf("levels Search Read Buf False: %s", err)
			return []byte{}, codec.NotFound
		}
		return value, codec.Found
	}
	return []byte{}, codec.NotFound
}

func (l *level) getEntry(sstIndex, keyIndex int) (*codec.Entry, bool) {
	sst := l.Sstable[sstIndex]
	if keyIndex >= len(sst.idxArea.Keys) {
		return &codec.Entry{}, false
	}

	key := sst.idxArea.Keys[keyIndex]
	postion := sst.idxArea.Pos[key]

	value := make([]byte, postion.Len)
	_, err := sst.f.(*file.MMapFile).Read(value, postion.Offset)
	if err != nil {
		return &codec.Entry{}, false
	}
	entry := codec.NewEntry(key, value)
	entry.Deleted = postion.Deleted
	return &entry, true
}

func (lm *levelManager) Search(key string) ([]byte, codec.Status) {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	for i := 0; i < len(lm.levels); i++ {
		e, status := lm.levels[i].search(key)
		if status == codec.Deleted {
			return []byte{}, codec.Deleted
		}
		if status == codec.Found {
			return e, codec.Found
		}
	}
	return []byte{}, codec.NotFound
}
