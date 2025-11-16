package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

const pageSize = 4096

type Page struct {
	Id   int
	Data []byte
}

type Pager struct {
	f *os.File
}

func (p *Pager) WritePage(pg *Page) error {
	offset := pg.Id * pageSize
	_, err := p.f.WriteAt(pg.Data, int64(offset))
	return err
}

func (p *Pager) ReadPage(id int64) (*Page, error) {
	offset := id * pageSize
	buf := make([]byte, pageSize)
	_, err := p.f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	return &Page{
		Id:   int(id),
		Data: buf,
	}, nil
}

func IntSliceToBytes(nums []int) []byte {
	buf := make([]byte, 4*len(nums))
	for i, n := range nums {
		binary.BigEndian.PutUint32(buf[i*4:], uint32(n))
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
	arr := make([]int, pageSize/4)
	for i := 0; i < pageSize/4; i++ {
		arr[i] = i
	}

	f, err := os.OpenFile("test.db", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	pager := &Pager{
		f: f,
	}

	bytes := IntSliceToBytes(arr)
	page := &Page{
		Id:   1,
		Data: bytes,
	}

	err = pager.WritePage(page)

	if err != nil {
		panic("Error!")
	}

	page, err = pager.ReadPage(int64(page.Id))

	if err != nil {
		panic(err)
	}

	ints := BytesToIntSlice(page.Data)
	fmt.Printf("Data length: %d bytes\n", len(page.Data))
	fmt.Printf("First 10 integers: %v\n", ints[:10])
	fmt.Printf("Last 10 integers: %v\n", ints[len(ints)-10:])
}
