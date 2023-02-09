package file

import "os"

type IOSelector interface {
	Close() error
	Sync() error
	Delete() error
	Write(buf []byte, offset int64) (int, error)
	Read(buf []byte, offset int64) (int, error)
	Truncature(size int64) error
	Size() int64
}

func openFile(fName string, fsize int64) (*os.File, error) {
	fd, err := os.OpenFile(fName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size() < fsize {
		if err := fd.Truncate(fsize); err != nil {
			return nil, err
		}
	}
	return fd, nil
}
