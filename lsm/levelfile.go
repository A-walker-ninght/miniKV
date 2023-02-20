package lsm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"

	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/file"
	"github.com/A-walker-ninght/miniKV/tools"
)

type levelFile struct {
	levelsfile []*levelfile
}

type levelfile struct {
	f            file.IOSelector
	filepath     string
	SSTablePaths []string
	p            int64 // 指针
}

func NewlevelFile() *levelFile {
	config := config.GetConfig()
	lvF := &levelFile{
		levelsfile: make([]*levelfile, config.MaxLevelNum),
	}
	lvF.initLevelFile(config.MaxLevelNum, config.LevelDir)
	return lvF
}

func (l *levelFile) initLevelFile(maxlv int, lvDir string) {
	lvs, _ := ioutil.ReadDir(lvDir)
	if len(lvs) == 0 {
		for i := 0; i < maxlv; i++ {
			fileName := strings.Builder{}
			fileName.WriteString("level_")
			fileName.WriteString(strconv.Itoa(i))
			fileName.WriteString(".log")

			filepath := tools.GetFilePath(lvDir, fileName.String())
			l.levelsfile[i] = newlevelfile(filepath)
		}
		return
	}
	for i, lv := range lvs {
		filepath := strings.Builder{}
		filepath.WriteString(lvDir)
		filepath.WriteString("/")
		filepath.WriteString(lv.Name())
		l.levelsfile[i] = newlevelfile(filepath.String())
	}
	return
}

func (l *levelFile) Write(sstpath string, lv int) {
	l.levelsfile[lv].Write(sstpath)
}

func (l *levelFile) Clearlv(lv int) {
	l.levelsfile[lv].Clear()
}

func newlevelfile(filepath string) *levelfile {
	lv := &levelfile{
		filepath: filepath,
	}
	lv.initlevelfile()
	return lv
}

func (lf *levelfile) initlevelfile() {
	stat, _ := os.Stat(lf.filepath)
	var size int64
	if stat == nil {
		size = int64(1000)
	} else {
		size = stat.Size()
	}
	fd, err := file.OpenMMapFile(lf.filepath, size)
	if err != nil {
		fmt.Println(err)
		return
	}
	lf.f = fd
	lf.SSTablePaths = make([]string, 0)
	for {
		bufLen := make([]byte, 8)
		n, _ := lf.f.(*file.MMapFile).Read(bufLen, lf.p)

		if n == 0 {
			break
		}

		lf.p += 8
		length := int64(binary.BigEndian.Uint64(bufLen))
		sstPath := make([]byte, length)
		n, _ = lf.f.(*file.MMapFile).Read(sstPath, lf.p)
		if n == 0 {
			lf.p -= 8
			break
		}
		var path string
		json.Unmarshal(sstPath, &path)
		lf.SSTablePaths = append(lf.SSTablePaths, path)
		lf.p += int64(n)
	}
}

func (lf *levelfile) Write(sstpath string) {
	lf.SSTablePaths = append(lf.SSTablePaths, sstpath)
	path, err := json.Marshal(sstpath)
	if err != nil {
		fmt.Errorf("LevelFile levelfile Write False")
		return
	}
	length := len(path)
	lengthbuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthbuf, uint64(length))
	lf.f.(*file.MMapFile).Write(lengthbuf, lf.p)

	lf.p += 8
	n, _ := lf.f.(*file.MMapFile).Write(path, lf.p)
	if n == 0 {
		return
	}
	lf.p += int64(n)
	err = lf.f.(*file.MMapFile).Sync()
}

func (lf *levelfile) Clear() {
	lf.f.(*file.MMapFile).Delete()
	f, err := file.OpenMMapFile(lf.filepath, 1000)
	if err != nil {
		fmt.Println(err)
	}
	lf.f = f
	lf.SSTablePaths = make([]string, 0)
	lf.p = 0
}
