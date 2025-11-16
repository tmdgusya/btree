package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var Magic = [4]byte{'L', 'L', 'S', 'T'}
var Endian = binary.BigEndian
var ErrInvalidMagic = errors.New("Invalid file: magic mismatch")

const PAGE_SIZE = 4096

// 페이지 헤더 크기 (byte). 여기서는 "Used" 값 하나만 둔다.
const PAGE_HEADER_SIZE = 2

// 슬롯(노드) 하나가 디스크에 차지하는 크기(byte).
// - Value: uint32 (4 바이트)
// - NextPage: uint32 (4 바이트)
// - NextSlot: uint16 (2 바이트)
// - Tomb: uint8 (1 바이트)
// - padding: uint8 (1 바이트)
const SLOT_SIZE = 12 // 4 + 4 + 2 + 1 + 1

// 한 페이지안에 들어갈 수 있는 Slot 개수
// 페이지 전체에서 페이지 헤더를 제외한 공간을 슬롯 크기로 나눔
const SLOTS_PER_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE

// 헤더의 고정 크기(바이트 단위)
// Magic(4 바이트) + Version(2 바이트) + PageSize(2 바이트) + PageCount(4 바이트)
// HeadPage(4 바이트) + HeadSlot(2 바이트) + TailPage(4 바이트) + TailSlot(2 바이트) + Size(8 바이트)
const HEADER_SIZE = 32 // 4 + 2 + 2 + 4 + 4 + 2 + 4 + 2 + 8

// 없음(NULL) 을 표기하기 위한 상수 값들
// uint32 의 모든 비트를 1로 세팅한 값 = 0xFFFFFFFF
// uint16 의 모든 비트를 1로 세팅한 값 = 0xFFFF
const NullPage uint32 = ^uint32(0)
const NullSlot uint16 = ^uint16(0)

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

// Used - 이 페이지에서 사용중인 슬롯 개수
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

func OpenPagedList(path string, truncate bool) (*os.File, *Header, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if info.Size() == 0 || truncate {
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

		if err := writeHeader(f, h); err != nil {
			f.Close()
			return nil, nil, err
		}

		return f, h, nil
	}

	header := &Header{}
	if err := readHeader(f, header); err != nil {
		f.Close()
		return nil, nil, err
	}

	return f, header, nil
}

func writeHeader(f *os.File, h *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
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

	if _, err := f.Write(buf); err != nil {
		return err
	}

	return nil
}

func readHeader(f *os.File, h *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, HEADER_SIZE)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}

	copy(h.Magic[:], buf[0:4])

	// Magic 검증
	if h.Magic != Magic {
		return ErrInvalidMagic
	}

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

// 페이지 슬롯 유틸리티
// - 헤더 영역(HeaderSize) 이후에 페이지들이 연속적으로 저장된다고 가정
// - pageID=0 이면 header 바로 뒤에 오는 첫 페이지
func pageOffset(pageID uint32) int64 {
	return int64(HEADER_SIZE) + int64(pageID)*PAGE_SIZE
}

// 새로운 빈 페이지를 파일에 생성
// - PageHeader(Used = 0) 으로 기록하고 나머지는 0 으로 채움
func initEmptyPage(f *os.File, pageID uint32) error {
	offset := pageOffset(pageID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	// 페이지 전체를 0 으로 채운다.
	buf := make([]byte, PAGE_SIZE)

	_, err := f.Write(buf)
	return err
}

func readPageHeader(f *os.File, pageID uint32) (PageHeader, error) {
	offset := pageOffset(pageID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return PageHeader{}, err
	}

	buf := make([]byte, PAGE_HEADER_SIZE)
	if _, err := io.ReadFull(f, buf); err != nil {
		return PageHeader{}, err
	}

	var ph PageHeader
	ph.Used = Endian.Uint16(buf[0:2])
	return ph, nil
}

func writePageHeader(f *os.File, pageID uint32, ph PageHeader) error {
	offset := pageOffset(pageID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, PAGE_HEADER_SIZE)
	Endian.PutUint16(buf[0:2], ph.Used)

	_, err := f.Write(buf)
	return err
}

// 특정 페이지/슬롯 위치에 Node 쓰기
// - 페이지 내 레이아웃: [PageHeader(2바이트)] [Slot 0] [Slot 1]
// - 특정 슬롯의 오프셋 = pageOffset + PAGE_HEADER_SIZE + SLOT_SIZE * slotID
func writeSlot(f *os.File, pageID uint32, slotID uint16, node Node) error {
	offset := pageOffset(pageID) + PAGE_HEADER_SIZE + SLOT_SIZE*int64(slotID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, SLOT_SIZE)
	Endian.PutUint32(buf[0:4], node.Value)
	Endian.PutUint32(buf[4:8], node.NextPage)
	Endian.PutUint16(buf[8:10], node.NextSlot)
	buf[10] = node.Tomb
	buf[11] = node._pad // 의미없는 패딩값 (0 유지)

	_, err := f.Write(buf)
	return err
}

func readSlot(f *os.File, pageID uint32, slotID uint16) (Node, error) {
	offset := pageOffset(pageID) + PAGE_HEADER_SIZE + SLOT_SIZE*int64(slotID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return Node{}, err
	}

	buf := make([]byte, SLOT_SIZE)
	if _, err := io.ReadFull(f, buf); err != nil {
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

// 새 슬롯을 할당하는 함수
// - 마지막 페이지가 존재하고 여유 슬롯이 있으면 그 페이지를 사용.
// - 마지막 페이지가 가득 찼으면 새 페이지를 생성하고 그 페이지의 0번 슬롯을 사용
// - Header 의 PageCount를 증가시킴
func allocateSlot(f *os.File, h *Header) (pageID uint32, slotIndex uint16, err error) {
	if h.PageCount == 0 {
		pageID = 0
		if err = initEmptyPage(f, pageID); err != nil {
			return
		}
		h.PageCount = 1
	} else {
		// 이미 페이지가 하나 이상 있으면, "마지막 페이지" 를 우선 사용
		pageID = h.PageCount - 1
	}

	ph, err := readPageHeader(f, pageID)

	if err != nil {
		return
	}

	if int(ph.Used) >= SLOTS_PER_PAGE {
		pageID = h.PageCount // 새 페이지 번호
		if err = initEmptyPage(f, pageID); err != nil {
			return
		}
		h.PageCount++
		ph.Used = 0
	}

	slotIndex = ph.Used
	ph.Used++
	if err = writePageHeader(f, pageID, ph); err != nil {
		return
	}
	return pageID, slotIndex, nil
}

func appendTail(f *os.File, h *Header, value uint32) error {
	pageID, slotIndex, err := allocateSlot(f, h)
	if err != nil {
		return err
	}

	slotOffset := pageOffset(pageID) + PAGE_HEADER_SIZE + SLOT_SIZE*int64(slotIndex)
	if _, err := f.Seek(slotOffset, io.SeekStart); err != nil {
		return err
	}

	newNode := &Node{
		Value:    value,
		NextPage: NullPage,
		NextSlot: NullSlot,
		Tomb:     0,
		_pad:     0,
	}

	if err := writeSlot(f, pageID, slotIndex, *newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.HeadPage = pageID
		h.HeadSlot = slotIndex
		h.TailPage = pageID
		h.TailSlot = slotIndex
		h.Size++
		return writeHeader(f, h)
	}

	tailNode, err := readSlot(f, h.TailPage, h.TailSlot)

	if err != nil {
		return err
	}

	tailNode.NextPage = pageID
	tailNode.NextSlot = slotIndex
	if err := writeSlot(f, h.TailPage, h.TailSlot, tailNode); err != nil {
		return err
	}

	h.TailPage = pageID
	h.TailSlot = slotIndex
	h.Size++
	return writeHeader(f, h)
}

func prependHead(f *os.File, h *Header, value uint32) error {
	pageID, slotIndex, err := allocateSlot(f, h)
	if err != nil {
		return err
	}

	newNode := &Node{
		Value:    value,
		NextPage: h.HeadPage,
		NextSlot: h.HeadSlot,
		Tomb:     0,
		_pad:     0,
	}

	if err := writeSlot(f, pageID, slotIndex, *newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.TailPage = pageID
		h.TailSlot = slotIndex
	}
	h.HeadPage = pageID
	h.HeadSlot = slotIndex
	h.Size++
	return writeHeader(f, h)
}

func traverseValues(f *os.File, h *Header) ([]uint32, error) {
	values := make([]uint32, 0, h.Size)

	page := h.HeadPage
	slot := h.HeadSlot

	for page != NullPage && slot != NullSlot {
		node, err := readSlot(f, page, slot)
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

func main() {
	// 교육용: 항상 새로 시작하도록 truncate=true
	f, h, err := OpenPagedList("paged_list.llst", true)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 머리에 2,1,0 추가: [2,1,0]
	if err := prependHead(f, h, 0); err != nil {
		panic(err)
	}
	if err := prependHead(f, h, 1); err != nil {
		panic(err)
	}
	if err := prependHead(f, h, 2); err != nil {
		panic(err)
	}

	// 꼬리에 3,4,5 추가: [2,1,0,3,4,5]
	if err := appendTail(f, h, 3); err != nil {
		panic(err)
	}
	if err := appendTail(f, h, 4); err != nil {
		panic(err)
	}
	if err := appendTail(f, h, 5); err != nil {
		panic(err)
	}

	// 리스트 전체를 순회해 값 출력
	vals, err := traverseValues(f, h)
	if err != nil {
		panic(err)
	}
	fmt.Println("paged list values:", vals)

	// 헤더를 다시 읽어와 상태 확인 (파일 재오픈 시나리오 흉내)
	if err := readHeader(f, h); err != nil {
		panic(err)
	}
	fmt.Printf("Header{PageCount=%d, Size=%d, Head=(%d,%d), Tail=(%d,%d)}\n",
		h.PageCount, h.Size, h.HeadPage, h.HeadSlot, h.TailPage, h.TailSlot)
}
