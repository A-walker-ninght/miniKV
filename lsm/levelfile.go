package lsm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
)

type levelFile struct {
	f      *os.File
	levels []levelPath
}

type levelPath struct {
	SSTablePaths []string
}

func (l *levelFile) InitLevelFile(filepath string) error {
	fd, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	stat, err := fd.Stat()
	if err != nil {
		return err
	}

	if err := fd.Truncate(stat.Size()); err != nil {
		return err
	}
	l.f = fd

	fileLenBuf := make([]byte, 8)
	l.f.ReadAt(fileLenBuf, 0)
	fileLen := int64(binary.BigEndian.Uint64(fileLenBuf))

	levelsBuf := make([]byte, fileLen)
	l.f.ReadAt(levelsBuf, 8)
	var levels []levelPath
	json.Unmarshal(levelsBuf, levels)
	l.levels = levels
	return nil
}

func (l *levelFile) Sync() error {
	levelsBuf, err := json.Marshal(l.levels)
	if err != nil {
		fmt.Errorf("levelFile Sync Marshal False: %s", err)
		return err
	}
	levelLen := len(levelsBuf)
	err = l.f.Truncate(0)
	if err != nil {
		fmt.Errorf("levelFile Sync Truncate False: %s", err)
		return err
	}
	levelLenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(levelLenBuf, uint64(levelLen))
	_, err = l.f.WriteAt(levelLenBuf, 0)

	if err != nil {
		fmt.Errorf("levelFile Sync levelLen Write False: %s", err)
		return err
	}
	_, err = l.f.WriteAt(levelsBuf, 8)
	if err != nil {
		fmt.Errorf("levelFile Sync levels Write False: %s", err)
		return err
	}
	return nil
}

func (l *levelFile) Tables() []levelPath {
	return l.levels
}

func (l *levelFile) Close() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	return nil
}
