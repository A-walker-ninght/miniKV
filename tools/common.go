package tools

import (
	"strings"
)

func GetFilePath(dir string, fileName string) string {
	s := strings.Builder{}
	s.WriteString(dir)
	s.WriteString("/")
	s.WriteString(fileName)
	return s.String()
}
