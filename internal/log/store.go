// Package log /*Record—the data stored in our log.
//Store—the file we store records in.
//Index—the file we store index entries in.
//Segment—the abstraction that ties a store and an index together.
//Log—the abstraction that ties all the segments together.
package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var enc = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func NewStore(f *os.File) (*store, error) {
	// maybe re-creating the store from file that has existing data
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append 写入数据到buf, 返回写入的字节数,写的起始偏移量
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// first write length
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// second write data
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// uint64 8Byte
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	// read data length
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// contains data
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
