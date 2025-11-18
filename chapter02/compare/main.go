package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// ==================================
// 공통: 포맷 정의
// ==================================

var Magic = [4]byte{'L', 'L', 'S', 'T'}
var Endian = binary.BigEndian

const PAGE_SIZE = 4096
const PAGE_HEADER_SIZE = 2

// Node on disk:
// Value    uint32 (4)
// NextPage uint32 (4)
// NextSlot uint16 (2)
// Tomb     uint8  (1)
// _pad     uint8  (1)
const SLOT_SIZE = 12

const HEADER_SIZE = 32 // Magic(4) + Version(2) + PageSize(2) + PageCount(4) + HeadPage(4) + HeadSlot(2) + TailPage(4) + TailSlot(2) + Size(8)

const SLOTS_PER_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE

const NullPage uint32 = ^uint32(0) // 0xFFFFFFFF
const NullSlot uint16 = ^uint16(0) // 0xFFFF

type Header struct {
	Magic     [4]byte
	Version   uint16
	PageSize  uint16
	PageCount uint32
	HeadPage  uint32
	HeadSlot  uint16
	TailPage  uint32
	TailSlot  uint16
	Size      uint64
}

type PageHeader struct {
	Used uint16
}

type Node struct {
	Value    uint32
	NextPage uint32
	NextSlot uint16
	Tomb     uint8
	_pad     uint8
}

// ==================================
// I/O 계측용 파일 래퍼
// ==================================

type IOMetrics struct {
	Reads  int64
	Writes int64
	Seeks  int64
}

type CountingFile struct {
	f  *os.File
	io IOMetrics
}

func NewCountingFile(f *os.File) *CountingFile {
	return &CountingFile{f: f}
}

func (cf *CountingFile) Read(p []byte) (int, error) {
	cf.io.Reads++
	return cf.f.Read(p)
}

func (cf *CountingFile) Write(p []byte) (int, error) {
	cf.io.Writes++
	return cf.f.Write(p)
}

func (cf *CountingFile) Seek(offset int64, whence int) (int64, error) {
	cf.io.Seeks++
	return cf.f.Seek(offset, whence)
}

func (cf *CountingFile) Close() error {
	return cf.f.Close()
}

func (cf *CountingFile) Metrics() IOMetrics {
	return cf.io
}

func (m IOMetrics) Diff(prev IOMetrics) IOMetrics {
	return IOMetrics{
		Reads:  m.Reads - prev.Reads,
		Writes: m.Writes - prev.Writes,
		Seeks:  m.Seeks - prev.Seeks,
	}
}

// ==================================
// 헤더 / 페이지 / 슬롯 유틸
// ==================================

func pageOffset(pageID uint32) int64 {
	return int64(HEADER_SIZE) + int64(pageID)*PAGE_SIZE
}

func writeHeader(cf *CountingFile, h *Header) error {
	if _, err := cf.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, 0, HEADER_SIZE)
	buf = append(buf, h.Magic[:]...)
	buf = Endian.AppendUint16(buf, h.Version)
	buf = Endian.AppendUint16(buf, h.PageSize)
	buf = Endian.AppendUint32(buf, h.PageCount)
	buf = Endian.AppendUint32(buf, h.HeadPage)
	buf = Endian.AppendUint16(buf, h.HeadSlot)
	buf = Endian.AppendUint32(buf, h.TailPage)
	buf = Endian.AppendUint16(buf, h.TailSlot)
	buf = Endian.AppendUint64(buf, h.Size)

	_, err := cf.Write(buf)
	return err
}

func readHeader(cf *CountingFile, h *Header) error {
	if _, err := cf.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, HEADER_SIZE)
	if _, err := io.ReadFull(cf, buf); err != nil {
		return err
	}
	copy(h.Magic[:], buf[0:4])
	h.Version = Endian.Uint16(buf[4:6])
	h.PageSize = Endian.Uint16(buf[6:8])
	h.PageCount = Endian.Uint32(buf[8:12])
	h.HeadPage = Endian.Uint32(buf[12:16])
	h.HeadSlot = Endian.Uint16(buf[16:18])
	h.TailPage = Endian.Uint32(buf[18:22])
	h.TailSlot = Endian.Uint16(buf[22:24])
	h.Size = Endian.Uint64(buf[24:32])
	return nil
}

func initEmptyPage(cf *CountingFile, pageID uint32) error {
	offset := pageOffset(pageID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, PAGE_SIZE) // zeroed page
	_, err := cf.Write(buf)
	return err
}

func readPageHeader(cf *CountingFile, pageID uint32) (PageHeader, error) {
	offset := pageOffset(pageID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return PageHeader{}, err
	}
	buf := make([]byte, PAGE_HEADER_SIZE)
	if _, err := io.ReadFull(cf, buf); err != nil {
		return PageHeader{}, err
	}
	var ph PageHeader
	ph.Used = Endian.Uint16(buf[0:2])
	return ph, nil
}

func writePageHeader(cf *CountingFile, pageID uint32, ph PageHeader) error {
	offset := pageOffset(pageID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, PAGE_HEADER_SIZE)
	Endian.PutUint16(buf[0:2], ph.Used)
	_, err := cf.Write(buf)
	return err
}

func writeSlot(cf *CountingFile, pageID uint32, slotID uint16, node Node) error {
	offset := pageOffset(pageID) + PAGE_HEADER_SIZE + int64(SLOT_SIZE)*int64(slotID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, SLOT_SIZE)
	Endian.PutUint32(buf[0:4], node.Value)
	Endian.PutUint32(buf[4:8], node.NextPage)
	Endian.PutUint16(buf[8:10], node.NextSlot)
	buf[10] = node.Tomb
	buf[11] = node._pad

	_, err := cf.Write(buf)
	return err
}

// naive: 슬롯 하나마다 Seek+Read
func readSlotNaive(cf *CountingFile, pageID uint32, slotID uint16) (Node, error) {
	offset := pageOffset(pageID) + PAGE_HEADER_SIZE + int64(SLOT_SIZE)*int64(slotID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return Node{}, err
	}

	buf := make([]byte, SLOT_SIZE)
	if _, err := io.ReadFull(cf, buf); err != nil {
		return Node{}, err
	}

	var node Node
	node.Value = Endian.Uint32(buf[0:4])
	node.NextPage = Endian.Uint32(buf[4:8])
	node.NextSlot = Endian.Uint16(buf[8:10])
	node.Tomb = buf[10]
	node._pad = buf[11]
	return node, nil
}

// ==================================
// PageBuffer: 페이지 단위 로딩 + 슬롯 메모리 파싱
// ==================================

type PageBuffer struct {
	pageID uint32
	data   []byte
	valid  bool
}

func (pb *PageBuffer) loadPage(cf *CountingFile, pageID uint32) error {
	offset := pageOffset(pageID)
	if _, err := cf.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	if pb.data == nil || len(pb.data) != PAGE_SIZE {
		pb.data = make([]byte, PAGE_SIZE)
	}
	if _, err := io.ReadFull(cf, pb.data); err != nil {
		return err
	}

	pb.pageID = pageID
	pb.valid = true
	return nil
}

func readSlotWithBuffer(cf *CountingFile, pb *PageBuffer, pageID uint32, slotID uint16) (Node, error) {
	// 필요할 때만 페이지 전체 읽기
	if !pb.valid || pb.pageID != pageID {
		if err := pb.loadPage(cf, pageID); err != nil {
			return Node{}, err
		}
	}

	start := PAGE_HEADER_SIZE + int64(SLOT_SIZE)*int64(slotID)
	end := start + SLOT_SIZE
	slotBytes := pb.data[start:end]

	var node Node
	node.Value = Endian.Uint32(slotBytes[0:4])
	node.NextPage = Endian.Uint32(slotBytes[4:8])
	node.NextSlot = Endian.Uint16(slotBytes[8:10])
	node.Tomb = slotBytes[10]
	node._pad = slotBytes[11]
	return node, nil
}

// ==================================
// 슬롯 할당 / AppendTail 로 리스트 구성
// ==================================

func allocateSlot(cf *CountingFile, h *Header) (pageID uint32, slotIndex uint16, err error) {
	if h.PageCount == 0 {
		pageID = 0
		if err = initEmptyPage(cf, pageID); err != nil {
			return
		}
		h.PageCount = 1
	} else {
		pageID = h.PageCount - 1
	}

	ph, err := readPageHeader(cf, pageID)
	if err != nil {
		return
	}

	if int(ph.Used) >= SLOTS_PER_PAGE {
		pageID = h.PageCount
		if err = initEmptyPage(cf, pageID); err != nil {
			return
		}
		h.PageCount++
		ph.Used = 0
	}

	slotIndex = ph.Used
	ph.Used++
	if err = writePageHeader(cf, pageID, ph); err != nil {
		return
	}
	return
}

// AppendTail only (append-only 리스트)
// 논리 순서 = 물리 삽입 순서
func appendTail(cf *CountingFile, h *Header, value uint32) error {
	pageID, slotIndex, err := allocateSlot(cf, h)
	if err != nil {
		return err
	}

	newNode := Node{
		Value:    value,
		NextPage: NullPage,
		NextSlot: NullSlot,
		Tomb:     0,
		_pad:     0,
	}

	if err := writeSlot(cf, pageID, slotIndex, newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.HeadPage = pageID
		h.HeadSlot = slotIndex
		h.TailPage = pageID
		h.TailSlot = slotIndex
		h.Size++
		return writeHeader(cf, h)
	}

	// 기존 tail 노드의 Next 를 새 슬롯으로 연결
	tailNode, err := readSlotNaive(cf, h.TailPage, h.TailSlot)
	if err != nil {
		return err
	}
	tailNode.NextPage = pageID
	tailNode.NextSlot = slotIndex
	if err := writeSlot(cf, h.TailPage, h.TailSlot, tailNode); err != nil {
		return err
	}

	h.TailPage = pageID
	h.TailSlot = slotIndex
	h.Size++
	return writeHeader(cf, h)
}

// ==================================
// Traverse: naive vs buffered
// ==================================

func traverseNaive(cf *CountingFile, h *Header) ([]uint32, error) {
	values := make([]uint32, 0, int(h.Size))
	page := h.HeadPage
	slot := h.HeadSlot

	for page != NullPage && slot != NullSlot {
		node, err := readSlotNaive(cf, page, slot)
		if err != nil {
			return nil, err
		}
		if node.Tomb == 0 {
			values = append(values, node.Value)
		}
		page = node.NextPage
		slot = node.NextSlot
	}
	return values, nil
}

func traverseBuffered(cf *CountingFile, h *Header) ([]uint32, error) {
	values := make([]uint32, 0, int(h.Size))
	page := h.HeadPage
	slot := h.HeadSlot

	var pb PageBuffer

	for page != NullPage && slot != NullSlot {
		node, err := readSlotWithBuffer(cf, &pb, page, slot)
		if err != nil {
			return nil, err
		}
		if node.Tomb == 0 {
			values = append(values, node.Value)
		}
		page = node.NextPage
		slot = node.NextSlot
	}
	return values, nil
}

// ==================================
// main: 리스트 하나 만들고 두 방식 비교
// ==================================

func main() {
	const N = 100000
	const path = "paged_buffer_compare.llst"

	// 깨끗하게 시작
	_ = os.Remove(path)

	raw, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		panic(err)
	}
	cf := NewCountingFile(raw)
	defer cf.Close()

	h := &Header{
		Magic:     Magic,
		Version:   2,
		PageSize:  PAGE_SIZE,
		PageCount: 0,
		HeadPage:  NullPage,
		HeadSlot:  NullSlot,
		TailPage:  NullPage,
		TailSlot:  NullSlot,
		Size:      0,
	}

	if err := writeHeader(cf, h); err != nil {
		panic(err)
	}

	// 리스트 구성: append-only
	for i := 0; i < N; i++ {
		if err := appendTail(cf, h, uint32(i)); err != nil {
			panic(err)
		}
	}

	// 헤더를 다시 읽어서 상태 확인
	if err := readHeader(cf, h); err != nil {
		panic(err)
	}
	fmt.Printf("List built: Size=%d, PageCount=%d\n", h.Size, h.PageCount)

	// ---------------------------
	// 1) naive Traverse
	// ---------------------------
	baseMetrics := cf.Metrics()
	valsNaive, err := traverseNaive(cf, h)
	if err != nil {
		panic(err)
	}
	afterNaive := cf.Metrics()
	naiveDelta := afterNaive.Diff(baseMetrics)

	fmt.Println("Naive traverse length:", len(valsNaive))
	fmt.Printf("Naive I/O: Reads=%d, Writes=%d, Seeks=%d\n",
		naiveDelta.Reads, naiveDelta.Writes, naiveDelta.Seeks)

	// ---------------------------
	// 2) buffered Traverse
	// ---------------------------
	baseMetrics2 := cf.Metrics()
	valsBuf, err := traverseBuffered(cf, h)
	if err != nil {
		panic(err)
	}
	afterBuf := cf.Metrics()
	bufDelta := afterBuf.Diff(baseMetrics2)

	fmt.Println("Buffered traverse length:", len(valsBuf))
	fmt.Printf("Buffered I/O: Reads=%d, Writes=%d, Seeks=%d\n",
		bufDelta.Reads, bufDelta.Writes, bufDelta.Seeks)

	// I want to print out the diff
	fmt.Printf("Buffered I/O Diff: Reads=%d, Writes=%d, Seeks=%d\n",
		bufDelta.Reads-naiveDelta.Reads,
		bufDelta.Writes-naiveDelta.Writes,
		bufDelta.Seeks-naiveDelta.Seeks)
}
