package lsm

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/tools"
)

type LSM struct {
	memTable   *Memtable
	immutables []*Memtable
	levels     *levelManager
	stopCh     chan struct{} // 关闭
	checkCh    chan struct{}
	lock       *sync.RWMutex
}

// 增删操作在memtable里完成。
// 增：略
// 删除：如果key存在，将Deleted = true; 如果没有key，则新增一条，并将Deleted = true
func NewLSM() *LSM {
	config := config.GetConfig()
	lsm := &LSM{
		lock:     &sync.RWMutex{},
		levels:   NewLevelManager(),
		stopCh:   make(chan struct{}, 0),
		checkCh:  make(chan struct{}, 1),
		memTable: NewMemTable("wal.log"),
	}
	imFiles, err := ioutil.ReadDir(config.WalDir)
	if err != nil {
		fmt.Errorf("LSM ImmuTable recover False: %s", err)
		return lsm
	}

	for _, imfile := range imFiles {
		if imfile.Name() == "wal.log" {
			continue
		}

		lsm.immutables = append(lsm.immutables, NewMemTable(imfile.Name()))
	}
	go lsm.MergeTicker()
	return lsm
}

func (l *LSM) MergeTicker() error {
	config := config.GetConfig()
	timer := time.NewTimer(config.CheckInterval)
	defer timer.Stop()

	for {
		select {
		case <-l.stopCh:
			return errors.New("LSM Close & MergeTicker Close!")
		case <-timer.C:
			l.checkCh <- struct{}{}
		case <-l.checkCh:
			go l.Check()
		default:
		}
	}
}

func (l *LSM) Search(key string) []byte {
	// 先找内存表
	e, status := l.memTable.Search(key)
	if status == codec.Deleted {
		return []byte{}
	}
	if status == codec.Found {
		return e
	}

	// 没找到，再找immutable
	l.lock.RLock()
	for i := len(l.immutables) - 1; i >= 0; i-- {
		e, status = l.immutables[i].Search(key)
		if status == codec.Deleted {
			l.lock.RUnlock()
			return []byte{}
		}
		if status == codec.Found {
			l.lock.RUnlock()
			return e
		}
	}
	l.lock.RUnlock()

	// 再去levels里找
	e, status = l.levels.Search(key)
	if status == codec.Deleted {
		return []byte{}
	}
	if status == codec.Found {
		return e
	}
	return []byte{}
}

func (l *LSM) Set(key string, value []byte) error {
	// 先插入内存表
	e := codec.NewEntry(key, value)
	err := l.memTable.Add(&e)
	if err != nil {
		fmt.Errorf("LSM Set Entry To MemTable False: %s", err)
		return err
	}

	// 超过阈值convert
	newM := l.memTable.Convert()
	if newM != nil {
		l.lock.Lock()
		l.immutables = append(l.immutables, newM)
		mem := NewMemTable("wal.log")
		l.memTable = mem
		l.lock.Unlock()
		return nil
	}

	return nil
}

func (l *LSM) Delete(key string) {
	// 先找内存表
	e := codec.NewEntry(key, []byte{})
	e.Deleted = true
	l.memTable.Delete(&e)
}

func (l *LSM) Close() {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		l.stopCh <- struct{}{}
		wg.Done()
	}()
	wg.Wait()
}

func (l *LSM) Check() {
	config := config.GetConfig()
	go l.AppendSSTableToZero()
	go l.levels.Merge(config.PartSize)
}

func (l *LSM) AppendSSTableToZero() error {
	l.lock.Lock()
	defer l.lock.Unlock()
	config := config.GetConfig()

	for _, immutable := range l.immutables {
		// 每个immutable生成一个sst文件追加到尾部
		sstPath := "sst_0_"
		fmt.Println(immutable)
		iter := immutable.s.NewSkiplistInterator()
		var data []codec.Entry
		idx := int(time.Now().Unix())
		// 将迭代器里的数据取出
		for iter.First(); iter.Valid(); iter.Next() {
			data = append(data, *iter.Entry())
		}

		p := strings.Builder{}
		p.WriteString(sstPath)
		p.WriteString(strconv.Itoa(idx))
		p.WriteString(".sst")

		filepath := tools.GetFilePath(config.DataDir, p.String())

		// 路径根据level来定，例如：level0 第一个sst_0_0.sst，内存表插入第一层
		sst, err := CreateNewSSTable(data, filepath, 100000)
		if err != nil {
			fmt.Errorf("AppendSSTable Create SST False: %s", err)
			return err
		}

		l.levels.levels[0].Sstable = append(l.levels.levels[0].Sstable, sst)
		l.levels.levelfile.Write(filepath, 0)
		l.levels.levels[0].LevelCount += 1
		immutable.wal.Reset()
	}
	l.immutables = []*Memtable{}
	return nil
}
