package lsm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/file"
)

type levelManager struct {
	levelfile     *levelFile
	levelfilepath string
	levels        []*level
	signal        chan struct{}
}

type level struct {
	sstable    []*SSTable
	levelCount int
}

func NewLevelManager(lvFilePath string, levelNum int) *levelManager {
	lm := &levelManager{
		levelfilepath: lvFilePath,
		levels:        make([]*level, levelNum),
		signal:        make(chan struct{}, 1),
	}
	lm.levelfile = NewLevelFile(levelNum)
	lm.levelfile.Sync()
	return lm
}

func (l *level) Search(key string) ([]byte, bool) {
	// 对每一层都进行二分查找，需要从后往前找，因为是追加的
	for i := len(l.sstable) - 1; i >= 0; i-- {
		// 判断key是否在sst的[min, max]之间
		sst := l.sstable[i]
		if key < sst.minKey || key > sst.maxKey {
			continue
		}

		// 布隆过滤器过滤key
		if !sst.idxArea.door.Check(key) {
			continue
		}

		// 通过[]key二分查找
		var position = Position{
			Offset: -1,
		}

		l, r := 0, len(sst.idxArea.keys)-1
		for l <= r {
			mid := l + (r-l)/2
			if sst.idxArea.keys[mid] == key {
				position = sst.idxArea.pos[key]
				if position.Deleted {
					return []byte{}, position.Deleted
				}
			} else if sst.idxArea.keys[mid] > key {
				r = mid - 1
			} else if sst.idxArea.keys[mid] < key {
				l = mid + 1
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
			return []byte{}, false
		}
		return value, true
	}
	return []byte{}, false
}

func (l *level) getEntry(sstIndex, keyIndex int) (*codec.Entry, error) {
	sst := l.sstable[sstIndex]
	if keyIndex == len(sst.idxArea.keys) {
		return &codec.Entry{}, nil
	}

	key := sst.idxArea.keys[keyIndex]
	postion := sst.idxArea.pos[key]

	value := make([]byte, postion.Len)
	_, err := sst.f.(*file.MMapFile).Read(value, postion.Offset)
	if err != nil {
		return &codec.Entry{}, errors.New("level mergeSorts Read value False!")
	}
	entry := codec.NewEntry(key, value)
	return &entry, nil
}

func (lm *levelManager) AppendSSTableToZero(immutables []*Memtable) error {
	// 每个immutable生成一个sst文件追加到尾部
	sstPath := "sst_0_"
	dir := config.CheckConfig().DataDir
	for _, immutable := range immutables {
		iter := immutable.s.NewSkiplistInterator()
		var data []*codec.Entry
		idx := len(lm.levels[0].sstable)

		// 将迭代器里的数据取出
		for iter.First(); iter.Valid(); iter.Next() {
			data = append(data, iter.Entry())
		}

		p := strings.Builder{}
		p.WriteString(dir)
		p.WriteString(sstPath)
		p.WriteString(strconv.Itoa(idx))
		p.WriteString(".sst")
		filepath := p.String()
		// 路径根据level来定，例如：level0 第一个sst_0_0.sst，内存表插入第一层
		sst, err := CreateNewSSTable(data, filepath, 100000)
		if err != nil {
			fmt.Errorf("AppendSSTable Create SST False: %s", err)
			return err
		}
		lm.levels[0].sstable = append(lm.levels[0].sstable, sst)
		lm.levelfile.levels[0].SSTablePaths = append(lm.levelfile.levels[0].SSTablePaths, filepath)
		lm.levels[0].levelCount += 1
	}

	// 增加完了，保存路径
	lm.levelfile.Sync()

	// 判断是否需要压缩？
	threshold := config.CheckConfig().PartSize
	if lm.levels[0].levelCount >= threshold {
		// 压缩？
		err := lm.Merge()
		if err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) Merge() error {
	for lv := 0; lv < len(lm.levels); lv++ {
		if lm.levels[lv].levelCount < config.CheckConfig().Threshold {
			return nil
		}
		indexs := lm.getMergeIndex(lv)
		err := lm.mergeSorts(indexs, lv)
		if err != nil {
			fmt.Errorf("levels levelManager Merge False: %s", err)
			return err
		}
	}
	return nil
}

// 获取对应层级需要合并的sst文件，前往后合并，即取第一个sst，然后取[minKey, maxKey]有交集的sst合并
func (lm *levelManager) getMergeIndex(lv int) []int {
	indexs := make([]int, 0)
	sst := lm.levels[lv].sstable[0]
	minKey, maxKey := sst.minKey, sst.maxKey
	for i := 1; i < len(lm.levels[lv].sstable); i++ {
		min, max := lm.levels[lv].sstable[i].minKey, lm.levels[lv].sstable[i].maxKey
		if min > maxKey || max < minKey {
			continue
		}
		indexs = append(indexs, i)
	}
	return indexs
}

// 通过index获取sst文件，多路归并排序，生成多个sst文件
// sst文件value不能超过Threshold，保证大小差不多
// 多个sst合并，可能产生多个sst文件，插入到下一层

// indexs也是顺序的，前面旧，后面新
// 因为内存表是跳表，没有相同的key，所以一个sst文件里key都是不同的
func (lm *levelManager) mergeSorts(indexs []int, lv int) error {
	l := lm.levels[lv]                          // 层级
	p := make([]int, len(indexs))               // 指针, key: value = sstNum: keyIndex
	threshold := config.CheckConfig().Threshold // 如果 len(data) == threshold, 开启线程，init新的sst文件追加到下一层的末尾
	// 从后往前合并，到Threshold，创建一个新sst，开启一个线程插入
	data := make([]heapData, 0)
	lm.signal <- struct{}{}
	newH := newHeap(len(p))

	// 第一轮，插入所有sst文件索引为0的key，对应的entry
	for i := 0; i < len(p); i++ {
		entry, err := l.getEntry(i, p[i])
		if err != nil {
			fmt.Errorf("level mergeSorts Read value False!")
			return nil
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

		// 如果发现相同的key，一定是不同的sst文件中的key，将sstIndex大的保留，因为sst是追加的
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

		// 当data达到阈值创建新的sst文件，插入下一层的尾部
		if len(data) >= threshold {
			select {
			case <-lm.signal:
				go lm.appendSSTableToLevel(data[:threshold], lv+1)
				if len(data) > threshold {
					data = data[threshold:]
				} else {
					data = make([]heapData, 0)
				}
			default:
				continue
			}
		}
	}
	for i := 0; i < len(indexs); i++ {
		err := lm.levels[lv].sstable[i].Remove()
		fmt.Errorf("levels levelManager mergeSorts Remove sstable false: %s", err)
		return err
	}
	return nil
}

// 追加到lv层末尾
func (lm *levelManager) appendSSTableToLevel(data []heapData, lv int) error {
	sstPath := fmt.Sprintf("sst_%d_%d.sst", lv, len(lm.levels[lv].sstable))
	entrys := make([]*codec.Entry, len(data))
	for i := 0; i < len(data); i++ {
		entrys[i] = data[i].entry
	}
	sst, err := CreateNewSSTable(entrys, sstPath, 10000)
	if err != nil {
		fmt.Errorf("levels levelManager AppendSSTableToLevel CreateNewSST False: %s", err)
		return err
	}
	lm.levels[lv].sstable = append(lm.levels[lv].sstable, sst)
	lm.levelfile.levels[lv].SSTablePaths = append(lm.levelfile.levels[lv].SSTablePaths, sstPath)
	lm.levelfile.Sync()
	lm.signal <- struct{}{}
	return nil
}
