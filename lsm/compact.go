package lsm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/tools"
)

func (lm *levelManager) Merge(threshold int) error {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	for lv := 0; lv < len(lm.levels); lv++ {
		size := int(lm.levels[lv].LevelSize() / 1024 / 1024)
		if lm.levels[lv].LevelCount > threshold || size > lm.levelSize.LSizes[lv] {
			err := lm.mergeSorts(lv, threshold)
			if err != nil {
				fmt.Errorf("levels levelManager Merge False: %s", err)
				return err
			}
		}
	}
	return nil

}

// 通过index获取sst文件，多路归并排序，生成多个sst文件
// sst文件value不能超过Threshold，保证大小差不多
// 多个sst合并，可能产生多个sst文件，插入到下一层

// indexs也是顺序的，前面旧，后面新
// 因为内存表是跳表，没有相同的key，所以一个sst文件里key都是不同的
func (lm *levelManager) mergeSorts(lv int, threshold int) error {
	l := lm.levels[lv]               // 层级
	p := make([]int, len(l.Sstable)) // 指针, key: value = sstNum: keyIndex
	if len(p) <= 1 {
		fmt.Errorf("LevelManager Merge mergeSorts level%d length is %d <= 1", lv, len(p))
		return nil
	}
	// 从后往前合并，到Threshold，创建一个新sst，开启一个线程插入
	data := make([]heapData, 0)
	newH := newHeap(len(p))

	// 第一轮，插入所有sst文件索引为0的key，对应的entry
	for i := 0; i < len(p); i++ {
		entry, f := l.getEntry(i, p[i])
		if !f {
			continue
		}
		h := heapData{entry, i}
		newH.Push(h)
		p[i]++
	}
	// 循环的取出顶层的data，然后将对应的sst文件指针后移
	for newH.Len() > 0 {
		topData := newH.Pop()
		// 如果data数组为空则直接添加，指针后移将第二个key添加进堆里
		if len(data) == 0 {
			data = append(data, topData)
			p[topData.index]++
			entry, _ := l.getEntry(topData.index, p[topData.index])
			if entry == nil {
				continue
			}
			newH.Push(heapData{entry, topData.index})
			continue
		}

		// 如果发现相同的key，index大的保留
		if data[len(data)-1].entry.Key == topData.entry.Key {
			if topData.index > data[len(data)-1].index {
				data[len(data)-1] = topData
			}
			p[topData.index]++
			entry, _ := l.getEntry(topData.index, p[topData.index])
			if entry == nil {
				continue
			}
			newH.Push(heapData{entry, topData.index})
			continue

		}
		// key不同，直接插入
		data = append(data, topData)
		p[topData.index]++
		entry, _ := l.getEntry(topData.index, p[topData.index])
		if entry == nil {
			continue
		}
		newH.Push(heapData{entry, topData.index})
	}
	level := lm.levels[lv]
	for i := 0; i < len(p); i++ {
		err := level.Sstable[i].Remove()
		fmt.Errorf("levels levelManager mergeSorts Remove sstable false: %s", err)
	}
	level.Sstable = []*SSTable{}
	if lv >= len(lm.levels)-1 {
		lm.levelfile.Clearlv(len(lm.levels) - 1)
		lm.appendSSTableToLevel(data, len(lm.levels)-1)
	} else {
		lm.appendSSTableToLevel(data, lv+1)
		lm.levelfile.Clearlv(lv)
	}
	return nil
}

// 追加到lv层末尾
func (lm *levelManager) appendSSTableToLevel(data []heapData, lv int) error {
	config := config.GetConfig()
	s := strings.Builder{}
	s.WriteString("sst_")
	s.WriteString(strconv.Itoa(lv))
	s.WriteString("_")
	s.WriteString(strconv.Itoa(int(time.Now().Unix())))
	s.WriteString(".sst")
	sstName := s.String()

	sstPath := tools.GetFilePath(config.DataDir, sstName)
	entrys := make([]codec.Entry, len(data))
	for i := 0; i < len(data); i++ {
		entrys[i] = *data[i].entry
	}
	sst, err := CreateNewSSTable(entrys, sstPath, 10000)
	if err != nil {
		fmt.Errorf("levels levelManager AppendSSTableToLevel CreateNewSST False: %s", err)
		return err
	}

	lm.levels[lv].Sstable = append(lm.levels[lv].Sstable, sst)
	lm.levelfile.Write(sstPath, lv)
	return nil
}
