package lsm

import (
	// "github.com/stretchr/testify/assert"
	"fmt"
	"testing"
)

func TestInit(t *testing.T) {
	l := NewlevelFile(7)
	l.Write("sst_0_1.sst", 0)
	fmt.Println(l.levelsfile[0].SSTablePaths)
	fmt.Println(l.levelsfile[0].p)
	l = NewlevelFile(7)
	fmt.Println(l.levelsfile[0].SSTablePaths)
	fmt.Println(l.levelsfile[0].p)
}
