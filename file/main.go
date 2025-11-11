package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

const PAGE_SIZE = 16 // Byte
const INT_LENGTH = 24
const BYTE_LENGTH = INT_LENGTH * 4

type Page struct {
	Id   int32
	Data []byte
}

type PageManager struct {
	f     *os.File
	pages []*Page
}

func (p *PageManager) ReadAt(id int32) []byte {
	return p.pages[id].Data
}

func (p *PageManager) ReadAll() error {
	for i := 0; i < BYTE_LENGTH/PAGE_SIZE; i++ {
		buf := make([]byte, PAGE_SIZE)
		p.f.Seek(int64(i*PAGE_SIZE), 0)
		p.f.Read(buf)
		p.pages[i] = &Page{
			Id:   int32(i),
			Data: buf,
		}
	}
	return nil
}

func IntSliceToBytes(nums []uint32) []byte {
	buf := make([]byte, 4*len(nums))
	for i, n := range nums {
		binary.BigEndian.PutUint32(buf[i*4:], n)
	}
	return buf
}

func BytesToIntSlice(buf []byte) []int {
	n := len(buf) / 4
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = int(binary.BigEndian.Uint32(buf[i*4:]))
	}
	return out
}

func main() {
	f, err := os.OpenFile("test.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}

	pageManager := &PageManager{
		f:     f,
		pages: make([]*Page, BYTE_LENGTH/PAGE_SIZE),
	}

	arr := make([]uint32, INT_LENGTH)

	for i := 0; i < INT_LENGTH; i++ {
		arr[i] = uint32(i)
	}

	_, err = f.Write(IntSliceToBytes(arr))
	if err != nil {
		panic(err)
	}

	f.Seek(0, 0)

	pageManager.ReadAll()

	fmt.Printf("%v\n", BytesToIntSlice(pageManager.ReadAt(0)))
}
