package lsm

import (
	// "github.com/stretchr/testify/assert"
	"fmt"
	"testing"
)

func TestInit(t *testing.T) {
	l := NewlevelFile()
	l.Write("sst_0_1.sst", 0)
	fmt.Println(l.levelsfile[0].SSTablePaths)
	fmt.Println(l.levelsfile[0].p)
	l = NewlevelFile()
	fmt.Println(l.levelsfile[0].SSTablePaths)
	fmt.Println(l.levelsfile[0].p)
}
