package file

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type WalFile struct {
	fd  *os.File
	buf []byte
	cap int64
}

func CreateNewWal(fileName string, fileSize int64) (IOSelector, error) {
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
	return &WalFile{buf: buf, fd: file}, nil
}

func (m *WalFile) Close() error {
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

func (m *WalFile) Delete() error {
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

func (m *WalFile) Sync() error {
	if m == nil {
		return nil
	}
	return Msync(m.buf)
}

func (m *WalFile) Write(buf []byte, offset int64) (int, error) {
	length := int64(len(buf))
	if length <= 0 {
		return 0, nil
	}
	// 超出容量，写入失败
	if offset < 0 || length+offset > m.cap {
		return 0, io.EOF
	}
	n := copy(m.buf[offset:], buf)
	return n, nil
}

func (m *WalFile) Read(buf []byte, offset int64) (int, error) {
	if offset < 0 || offset >= m.cap {
		return 0, io.EOF
	}
	if offset+int64(len(buf)) > m.cap {
		return 0, io.EOF
	}
	n := copy(buf, m.buf[offset:])
	return n, nil
}
