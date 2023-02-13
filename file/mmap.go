package file

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// mmap内存映射
type MMapFile struct {
	buf []byte
	fd  *os.File
	cap int64
}

// 打开或创建
func OpenMMapFile(fileName string, fileSize int64) (IOSelector, error) {
	if fileSize <= 0 {
		return nil, errors.New(fmt.Sprintf("unable to open: %s", fileName))
	}
	file, err := openFile(fileName, fileSize)
	if err != nil {
		return nil, err
	}
	buf, err := Mmap(file, true, fileSize)
	if err != nil {
		return nil, err
	}
	return &MMapFile{buf: buf, fd: file, cap: fileSize}, nil
}

func (m *MMapFile) Close() error {
	if m.fd == nil {
		return nil
	}
	if err := Msync(m.buf); err != nil {
		return nil
	}
	if err := Munmap(m.buf); err != nil {
		return nil
	}
	return m.fd.Close()
}

func (m *MMapFile) Delete() error {
	if m.fd == nil {
		return nil
	}
	if err := Munmap(m.buf); err != nil {
		return err
	}
	m.buf = nil
	if err := m.fd.Truncate(0); err != nil {
		return err
	}
	if err := m.fd.Close(); err != nil {
		return err
	}
	return os.Remove(m.fd.Name())
}

func (m *MMapFile) Sync() error {
	if m == nil {
		return nil
	}

	return Msync(m.buf)
}

const MB = 1 << 24 // 16MB

// 写入一个buffer，空间不足则扩容
func (m *MMapFile) Write(buf []byte, offset int64) (int, error) {
	length := int64(len(buf))
	if length <= 0 {
		return 0, nil
	}
	// 写入失败
	if offset < 0 {
		return 0, io.EOF
	}
	// 扩容
	for length+offset > m.cap {
		growBy := len(m.buf)
		if growBy > MB {
			growBy = MB
		}
		if growBy < len(m.buf) {
			growBy = len(m.buf)
		}

		if err := m.Truncature(int64(len(m.buf) + growBy)); err != nil {
			return 0, err
		}
	}
	dLen := copy(m.buf[offset:offset+length], buf)
	if dLen != int(length) {
		return 0, errors.New("dLen != needSize AppendBuffer failed")
	}
	return dLen, nil
}

func (m *MMapFile) Read(buf []byte, offset int64) (int, error) {
	if offset < 0 || offset >= m.cap {
		return 0, io.EOF
	}
	if offset+int64(len(buf)) > m.cap {
		return 0, io.EOF
	}
	n := copy(buf, m.buf[offset:])
	return n, nil
}

func (m *MMapFile) Truncature(size int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.fd.Name(), err)
	}
	if err := m.fd.Truncate(size); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.fd.Name(), err)
	}

	var err error
	m.buf, err = Mremap(m.buf, int(size)) // Mmap up to max size.
	m.cap = int64(size)
	return err
}

func (m *MMapFile) Size() int64 {
	info, err := m.fd.Stat()
	if err != nil {
		fmt.Errorf("Get MMapFile Size False: %s", err)
		return 0
	}
	return info.Size()
}
