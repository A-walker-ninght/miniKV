package tools

import (
	"strconv"
	"strings"
)

type Opt struct {
	Mem       bool
	Immutable bool
}

func GetFilePath(dir string, fileName string) string {
	s := strings.Builder{}
	s.WriteString(dir)
	s.WriteString("/")
	s.WriteString(fileName)
	return s.String()
}

func GetFileName(opt Opt, id int) string {
	switch {
	case opt.Immutable:
		s := strings.Builder{}
		s.WriteString("wal_")
		s.WriteString(strconv.Itoa(id))
		s.WriteString(".iog")
		return s.String()
	case opt.Mem:
		s := strings.Builder{}
		s.WriteString("wal_")
		s.WriteString(strconv.Itoa(id))
		s.WriteString(".log")
		return s.String()
	default:
		return "InValid FilaName"
	}
}
