package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

// ==================================
// 공통 인터페이스 및 타입
// ==================================

type LinkedListStore interface {
	Open(path string, truncate bool) (*Handle, error)
	AppendTail(h *Handle, value uint32) error
	Where(h *Handle, target uint32) (int64, error)
	Close(h *Handle) error
}

type HeaderRecord interface {
	headerVersion() uint16
}

type Handle struct {
	File   *os.File
	Header HeaderRecord
}

var Magic = [4]byte{'C', 'M', 'P', 'R'}
var Endian = binary.BigEndian
var ErrInvalidMagic = errors.New("Invalid file: magic mismatch")

const NullOffset int64 = -1
const DefaultPageSize uint16 = 4096

// ==================================
// 1. Offset-Based Linked List
// ==================================

const offsetNodeSize = 16 // Value(4) + Next(8) + Tomb(1) + _pad(3)

type OffsetHeader struct {
	Magic      [4]byte
	Version    uint16
	PageSize   uint16
	HeadOffset int64
	TailOffset int64
	Size       int64
}

func (h *OffsetHeader) headerVersion() uint16 { return h.Version }

type OffsetNode struct {
	Value uint32
	Next  int64
	Tomb  uint8
	_pad  [3]byte
}

type OffsetStore struct{}

func (s *OffsetStore) Open(path string, truncate bool) (*Handle, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if info.Size() == 0 || truncate {
		hdr := &OffsetHeader{
			Magic:      Magic,
			Version:    1,
			PageSize:   DefaultPageSize,
			HeadOffset: NullOffset,
			TailOffset: NullOffset,
			Size:       0,
		}
		if err := writeOffsetHeader(f, hdr); err != nil {
			f.Close()
			return nil, err
		}
	}

	hrd := &OffsetHeader{}
	if err := readOffsetHeader(f, hrd); err != nil {
		f.Close()
		return nil, err
	}

	return &Handle{File: f, Header: hrd}, nil
}

func writeOffsetHeader(f *os.File, hdr *OffsetHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 0, 32)
	buf = append(buf, hdr.Magic[:]...)
	buf = Endian.AppendUint16(buf, hdr.Version)
	buf = Endian.AppendUint16(buf, hdr.PageSize)
	buf = Endian.AppendUint64(buf, uint64(hdr.HeadOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.TailOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.Size))
	_, err := f.Write(buf)
	return err
}

func readOffsetHeader(f *os.File, h *OffsetHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 32)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	copy(h.Magic[:], buf[0:4])
	h.Version = Endian.Uint16(buf[4:6])
	h.PageSize = Endian.Uint16(buf[6:8])
	h.HeadOffset = int64(Endian.Uint64(buf[8:16]))
	h.TailOffset = int64(Endian.Uint64(buf[16:24]))
	h.Size = int64(Endian.Uint64(buf[24:32]))
	return nil
}

func writeOffsetNodeAt(f *os.File, off int64, n *OffsetNode) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, offsetNodeSize)
	Endian.PutUint32(buf[0:4], n.Value)
	Endian.PutUint64(buf[4:12], uint64(n.Next))
	buf[12] = n.Tomb
	_, err := f.Write(buf)
	return err
}

func readOffsetNodeAt(f *os.File, off int64) (*OffsetNode, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, offsetNodeSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	n := &OffsetNode{
		Value: Endian.Uint32(buf[0:4]),
		Next:  int64(Endian.Uint64(buf[4:12])),
		Tomb:  buf[12],
	}
	return n, nil
}

func (s *OffsetStore) AppendTail(handle *Handle, value uint32) error {
	h := handle.Header.(*OffsetHeader)
	f := handle.File

	newNode := &OffsetNode{Value: value, Next: NullOffset, Tomb: 0}
	newOff, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := writeOffsetNodeAt(f, newOff, newNode); err != nil {
		return err
	}

	if h.HeadOffset == NullOffset {
		h.HeadOffset = newOff
		h.TailOffset = newOff
		h.Size++
		return writeOffsetHeader(f, h)
	}

	tailNode, err := readOffsetNodeAt(f, h.TailOffset)
	if err != nil {
		return err
	}
	tailNode.Next = newOff
	if err := writeOffsetNodeAt(f, h.TailOffset, tailNode); err != nil {
		return err
	}

	h.TailOffset = newOff
	h.Size++
	return writeOffsetHeader(f, h)
}

func (s *OffsetStore) Where(handle *Handle, target uint32) (int64, error) {
	h := handle.Header.(*OffsetHeader)
	f := handle.File

	off := h.HeadOffset
	for off != NullOffset {
		node, err := readOffsetNodeAt(f, off)
		if err != nil {
			return NullOffset, err
		}
		if node.Tomb == 0 && node.Value == target {
			return off, nil
		}
		off = node.Next
	}
	return NullOffset, nil
}

func (s *OffsetStore) Close(h *Handle) error {
	return h.File.Close()
}

// ==================================
// 2. Page-Slotted Linked List
// ==================================

const PAGE_SIZE = 4096
const PAGE_HEADER_SIZE = 2
const SLOT_SIZE = 12
const SLOTS_PER_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE
const HEADER_SIZE = 32

const NullPage uint32 = ^uint32(0)
const NullSlot uint16 = ^uint16(0)

type PagedHeader struct {
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

func (h *PagedHeader) headerVersion() uint16 { return h.Version }

type PageHeader struct {
	Used uint16
}

type PagedNode struct {
	Value    uint32
	NextPage uint32
	NextSlot uint16
	Tomb     uint8
	_pad     uint8
}

type PagedStore struct{}

type PageBuffer struct {
	pageID uint32
	data   []byte
	valid  bool
}

func pageOffset(pageID uint32) int64 {
	return int64(HEADER_SIZE) + int64(pageID)*PAGE_SIZE
}

func (s *PagedStore) Open(path string, truncate bool) (*Handle, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if info.Size() == 0 || truncate {
		h := &PagedHeader{
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
		if err := writePagedHeader(f, h); err != nil {
			f.Close()
			return nil, err
		}
		return &Handle{File: f, Header: h}, nil
	}

	header := &PagedHeader{}
	if err := readPagedHeader(f, header); err != nil {
		f.Close()
		return nil, err
	}

	return &Handle{File: f, Header: header}, nil
}

func writePagedHeader(f *os.File, h *PagedHeader) error {
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
	_, err := f.Write(buf)
	return err
}

func readPagedHeader(f *os.File, h *PagedHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, HEADER_SIZE)
	if _, err := io.ReadFull(f, buf); err != nil {
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

func initEmptyPage(f *os.File, pageID uint32) error {
	offset := pageOffset(pageID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
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

func writeSlot(f *os.File, pageID uint32, slotID uint16, node PagedNode) error {
	offset := pageOffset(pageID) + PAGE_HEADER_SIZE + SLOT_SIZE*int64(slotID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, SLOT_SIZE)
	Endian.PutUint32(buf[0:4], node.Value)
	Endian.PutUint32(buf[4:8], node.NextPage)
	Endian.PutUint16(buf[8:10], node.NextSlot)
	buf[10] = node.Tomb
	buf[11] = node._pad
	_, err := f.Write(buf)
	return err
}

func (pb *PageBuffer) loadPage(f *os.File, pageID uint32) error {
	offset := pageOffset(pageID)
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	if pb.data == nil || len(pb.data) != PAGE_SIZE {
		pb.data = make([]byte, PAGE_SIZE)
	}
	if _, err := io.ReadFull(f, pb.data); err != nil {
		return err
	}
	pb.pageID = pageID
	pb.valid = true
	return nil
}

func readSlotWithBuffer(f *os.File, pb *PageBuffer, pageID uint32, slotID uint16) (PagedNode, error) {
	if !pb.valid || pb.pageID != pageID {
		if err := pb.loadPage(f, pageID); err != nil {
			return PagedNode{}, err
		}
	}
	start := PAGE_HEADER_SIZE + int64(SLOT_SIZE)*int64(slotID)
	slotBytes := pb.data[start : start+SLOT_SIZE]

	var node PagedNode
	node.Value = Endian.Uint32(slotBytes[0:4])
	node.NextPage = Endian.Uint32(slotBytes[4:8])
	node.NextSlot = Endian.Uint16(slotBytes[8:10])
	node.Tomb = slotBytes[10]
	node._pad = slotBytes[11]
	return node, nil
}

func allocateSlot(f *os.File, h *PagedHeader) (pageID uint32, slotIndex uint16, err error) {
	if h.PageCount == 0 {
		pageID = 0
		if err = initEmptyPage(f, pageID); err != nil {
			return
		}
		h.PageCount = 1
	} else {
		pageID = h.PageCount - 1
	}

	ph, err := readPageHeader(f, pageID)
	if err != nil {
		return
	}

	if int(ph.Used) >= SLOTS_PER_PAGE {
		pageID = h.PageCount
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

func (s *PagedStore) AppendTail(handle *Handle, value uint32) error {
	h := handle.Header.(*PagedHeader)
	f := handle.File

	pageID, slotIndex, err := allocateSlot(f, h)
	if err != nil {
		return err
	}

	newNode := PagedNode{
		Value:    value,
		NextPage: NullPage,
		NextSlot: NullSlot,
		Tomb:     0,
		_pad:     0,
	}

	if err := writeSlot(f, pageID, slotIndex, newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.HeadPage = pageID
		h.HeadSlot = slotIndex
		h.TailPage = pageID
		h.TailSlot = slotIndex
		h.Size++
		return writePagedHeader(f, h)
	}

	var pb PageBuffer
	tailNode, err := readSlotWithBuffer(f, &pb, h.TailPage, h.TailSlot)
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
	return writePagedHeader(f, h)
}

func (s *PagedStore) Where(handle *Handle, target uint32) (int64, error) {
	h := handle.Header.(*PagedHeader)
	f := handle.File

	page := h.HeadPage
	slot := h.HeadSlot

	var pb PageBuffer

	for page != NullPage && slot != NullSlot {
		node, err := readSlotWithBuffer(f, &pb, page, slot)
		if err != nil {
			return NullOffset, err
		}

		if node.Tomb == 0 && node.Value == target {
			// 페이지+슬롯을 offset으로 변환 (비교를 위해)
			return pageOffset(page) + PAGE_HEADER_SIZE + SLOT_SIZE*int64(slot), nil
		}
		page = node.NextPage
		slot = node.NextSlot
	}

	return NullOffset, nil
}

func (s *PagedStore) Close(h *Handle) error {
	return h.File.Close()
}

// ==================================
// 3. Binary Search Tree
// ==================================

const bstNodeSize = 28 // Value(4) + Left(8) + Right(8) + Tomb(1) + _pad(7)

type BSTHeader struct {
	Magic      [4]byte
	Version    uint16
	PageSize   uint16
	RootOffset int64
	Size       int64
}

func (h *BSTHeader) headerVersion() uint16 { return h.Version }

type BSTNode struct {
	Value uint32
	Left  int64
	Right int64
	Tomb  uint8
	_pad  [7]byte
}

type BinaryTreeStore struct{}

func (s *BinaryTreeStore) Open(path string, truncate bool) (*Handle, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	if info.Size() == 0 || truncate {
		hdr := &BSTHeader{
			Magic:      Magic,
			Version:    3,
			PageSize:   DefaultPageSize,
			RootOffset: NullOffset,
			Size:       0,
		}
		if err := writeBSTHeader(f, hdr); err != nil {
			f.Close()
			return nil, err
		}
	}

	hrd := &BSTHeader{}
	if err := readBSTHeader(f, hrd); err != nil {
		f.Close()
		return nil, err
	}

	return &Handle{File: f, Header: hrd}, nil
}

func writeBSTHeader(f *os.File, hdr *BSTHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 0, 24)
	buf = append(buf, hdr.Magic[:]...)
	buf = Endian.AppendUint16(buf, hdr.Version)
	buf = Endian.AppendUint16(buf, hdr.PageSize)
	buf = Endian.AppendUint64(buf, uint64(hdr.RootOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.Size))
	_, err := f.Write(buf)
	return err
}

func readBSTHeader(f *os.File, h *BSTHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 24)
	if _, err := io.ReadFull(f, buf); err != nil {
		return err
	}
	copy(h.Magic[:], buf[0:4])
	h.Version = Endian.Uint16(buf[4:6])
	h.PageSize = Endian.Uint16(buf[6:8])
	h.RootOffset = int64(Endian.Uint64(buf[8:16]))
	h.Size = int64(Endian.Uint64(buf[16:24]))
	return nil
}

func writeBSTNodeAt(f *os.File, off int64, n *BSTNode) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, bstNodeSize)
	Endian.PutUint32(buf[0:4], n.Value)
	Endian.PutUint64(buf[4:12], uint64(n.Left))
	Endian.PutUint64(buf[12:20], uint64(n.Right))
	buf[20] = n.Tomb
	_, err := f.Write(buf)
	return err
}

func readBSTNodeAt(f *os.File, off int64) (*BSTNode, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	buf := make([]byte, bstNodeSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	n := &BSTNode{
		Value: Endian.Uint32(buf[0:4]),
		Left:  int64(Endian.Uint64(buf[4:12])),
		Right: int64(Endian.Uint64(buf[12:20])),
		Tomb:  buf[20],
	}
	return n, nil
}

func insertBSTNode(f *os.File, nodeOffset int64, value uint32) (int64, error) {
	if nodeOffset == NullOffset {
		newOffset, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return NullOffset, err
		}
		newNode := &BSTNode{
			Value: value,
			Left:  NullOffset,
			Right: NullOffset,
			Tomb:  0,
		}
		if err := writeBSTNodeAt(f, newOffset, newNode); err != nil {
			return NullOffset, err
		}
		return newOffset, nil
	}

	node, err := readBSTNodeAt(f, nodeOffset)
	if err != nil {
		return NullOffset, err
	}

	if value < node.Value {
		newLeft, err := insertBSTNode(f, node.Left, value)
		if err != nil {
			return NullOffset, err
		}
		node.Left = newLeft
		if err := writeBSTNodeAt(f, nodeOffset, node); err != nil {
			return NullOffset, err
		}
	} else if value > node.Value {
		newRight, err := insertBSTNode(f, node.Right, value)
		if err != nil {
			return NullOffset, err
		}
		node.Right = newRight
		if err := writeBSTNodeAt(f, nodeOffset, node); err != nil {
			return NullOffset, err
		}
	}

	return nodeOffset, nil
}

func (s *BinaryTreeStore) AppendTail(handle *Handle, value uint32) error {
	h := handle.Header.(*BSTHeader)
	f := handle.File

	newRoot, err := insertBSTNode(f, h.RootOffset, value)
	if err != nil {
		return err
	}

	h.RootOffset = newRoot
	h.Size++

	return writeBSTHeader(f, h)
}

func searchBSTNode(f *os.File, nodeOffset int64, target uint32) (int64, error) {
	if nodeOffset == NullOffset {
		return NullOffset, nil
	}

	node, err := readBSTNodeAt(f, nodeOffset)
	if err != nil {
		return NullOffset, err
	}

	if node.Tomb == 0 && node.Value == target {
		return nodeOffset, nil
	}

	if target < node.Value {
		return searchBSTNode(f, node.Left, target)
	}
	return searchBSTNode(f, node.Right, target)
}

func (s *BinaryTreeStore) Where(handle *Handle, target uint32) (int64, error) {
	h := handle.Header.(*BSTHeader)
	f := handle.File
	return searchBSTNode(f, h.RootOffset, target)
}

func (s *BinaryTreeStore) Close(h *Handle) error {
	return h.File.Close()
}

// ==================================
// 성능 측정
// ==================================

func benchmarkInsert(store LinkedListStore, path string, values []uint32) time.Duration {
	handle, err := store.Open(path, true)
	if err != nil {
		panic(err)
	}
	defer store.Close(handle)

	start := time.Now()
	for _, v := range values {
		if err := store.AppendTail(handle, v); err != nil {
			panic(err)
		}
	}
	return time.Since(start)
}

func benchmarkSearch(store LinkedListStore, path string, searchValues []uint32) (time.Duration, int) {
	handle, err := store.Open(path, false)
	if err != nil {
		panic(err)
	}
	defer store.Close(handle)

	found := 0
	start := time.Now()
	for _, v := range searchValues {
		offset, err := store.Where(handle, v)
		if err != nil {
			panic(err)
		}
		if offset != NullOffset {
			found++
		}
	}
	elapsed := time.Since(start)
	return elapsed, found
}

func main() {
	const N = 10000           // 데이터 크기
	const SearchCount = 1000  // 검색 횟수

	fmt.Println("=== 자료구조 성능 비교 ===")
	fmt.Printf("데이터 크기: %d\n", N)
	fmt.Printf("검색 횟수: %d\n\n", SearchCount)

	// 테스트 데이터 생성 (순차)
	valuesSequential := make([]uint32, N)
	for i := 0; i < N; i++ {
		valuesSequential[i] = uint32(i)
	}

	// 테스트 데이터 생성 (랜덤 - Binary Tree 균형을 위해)
	valuesRandom := make([]uint32, N)
	copy(valuesRandom, valuesSequential)
	rand.Seed(42) // 재현 가능하도록 고정 seed
	rand.Shuffle(len(valuesRandom), func(i, j int) {
		valuesRandom[i], valuesRandom[j] = valuesRandom[j], valuesRandom[i]
	})

	// 검색할 값들 (랜덤)
	rand.Seed(time.Now().UnixNano())
	searchValues := make([]uint32, SearchCount)
	for i := 0; i < SearchCount; i++ {
		searchValues[i] = uint32(rand.Intn(N))
	}

	// 파일 경로
	offsetPath := "compare_offset.llst"
	pagedPath := "compare_paged.llst"
	bstPath := "compare_bst.llst"

	// 기존 파일 삭제
	os.Remove(offsetPath)
	os.Remove(pagedPath)
	os.Remove(bstPath)

	// 1. Offset-Based Linked List
	fmt.Println("1. Offset-Based Linked List")
	fmt.Println("   시간 복잡도: 삽입 O(1), 검색 O(N)")
	offsetStore := &OffsetStore{}
	insertTime1 := benchmarkInsert(offsetStore, offsetPath, valuesSequential)
	fmt.Printf("   삽입 시간: %v\n", insertTime1)
	searchTime1, found1 := benchmarkSearch(offsetStore, offsetPath, searchValues)
	fmt.Printf("   검색 시간: %v (검색 성공: %d/%d)\n", searchTime1, found1, SearchCount)
	fmt.Printf("   평균 검색 시간: %v\n\n", searchTime1/time.Duration(SearchCount))

	// 2. Page-Slotted Linked List
	fmt.Println("2. Page-Slotted Linked List")
	fmt.Println("   시간 복잡도: 삽입 O(1), 검색 O(N)")
	fmt.Println("   (페이지 버퍼로 I/O 최적화)")
	pagedStore := &PagedStore{}
	insertTime2 := benchmarkInsert(pagedStore, pagedPath, valuesSequential)
	fmt.Printf("   삽입 시간: %v\n", insertTime2)
	searchTime2, found2 := benchmarkSearch(pagedStore, pagedPath, searchValues)
	fmt.Printf("   검색 시간: %v (검색 성공: %d/%d)\n", searchTime2, found2, SearchCount)
	fmt.Printf("   평균 검색 시간: %v\n\n", searchTime2/time.Duration(SearchCount))

	// 3. Binary Search Tree (순차 삽입 - 최악의 경우)
	fmt.Println("3. Binary Search Tree - 순차 삽입 (최악)")
	fmt.Println("   시간 복잡도: 삽입 O(N), 검색 O(N)")
	fmt.Println("   (순차 삽입으로 완전 불균형 트리 → Linked List처럼 퇴화)")
	bstStore := &BinaryTreeStore{}
	insertTime3 := benchmarkInsert(bstStore, bstPath, valuesSequential)
	fmt.Printf("   삽입 시간: %v\n", insertTime3)
	searchTime3, found3 := benchmarkSearch(bstStore, bstPath, searchValues)
	fmt.Printf("   검색 시간: %v (검색 성공: %d/%d)\n", searchTime3, found3, SearchCount)
	fmt.Printf("   평균 검색 시간: %v\n\n", searchTime3/time.Duration(SearchCount))

	// 4. Binary Search Tree (랜덤 삽입 - 균형잡힌 경우)
	fmt.Println("4. Binary Search Tree - 랜덤 삽입 (균형)")
	fmt.Println("   시간 복잡도: 삽입 O(log N), 검색 O(log N)")
	fmt.Println("   (랜덤 삽입으로 균형잡힌 트리 → 진짜 성능)")
	bstBalancedPath := "compare_bst_balanced.llst"
	os.Remove(bstBalancedPath)
	insertTime4 := benchmarkInsert(bstStore, bstBalancedPath, valuesRandom)
	fmt.Printf("   삽입 시간: %v\n", insertTime4)
	searchTime4, found4 := benchmarkSearch(bstStore, bstBalancedPath, searchValues)
	fmt.Printf("   검색 시간: %v (검색 성공: %d/%d)\n", searchTime4, found4, SearchCount)
	fmt.Printf("   평균 검색 시간: %v\n\n", searchTime4/time.Duration(SearchCount))

	// 비교 요약
	fmt.Println("=== 성능 비교 요약 ===")
	fmt.Printf("삽입 성능:\n")
	fmt.Printf("  Offset-Based LL:           %v (기준)\n", insertTime1)
	fmt.Printf("  Page-Slotted LL:           %v (%.2fx)\n", insertTime2, float64(insertTime2)/float64(insertTime1))
	fmt.Printf("  BST (순차/불균형):         %v (%.2fx) ⚠️ 최악!\n", insertTime3, float64(insertTime3)/float64(insertTime1))
	fmt.Printf("  BST (랜덤/균형):           %v (%.2fx)\n\n", insertTime4, float64(insertTime4)/float64(insertTime1))

	fmt.Printf("검색 성능 (평균):\n")
	avgSearch1 := searchTime1 / time.Duration(SearchCount)
	avgSearch2 := searchTime2 / time.Duration(SearchCount)
	avgSearch3 := searchTime3 / time.Duration(SearchCount)
	avgSearch4 := searchTime4 / time.Duration(SearchCount)
	fmt.Printf("  Offset-Based LL:           %v (기준)\n", avgSearch1)
	fmt.Printf("  Page-Slotted LL:           %v (%.2fx) ✓ I/O 최적화\n", avgSearch2, float64(avgSearch2)/float64(avgSearch1))
	fmt.Printf("  BST (순차/불균형):         %v (%.2fx) ⚠️ LL과 비슷\n", avgSearch3, float64(avgSearch3)/float64(avgSearch1))
	fmt.Printf("  BST (랜덤/균형):           %v (%.2fx) ✓ 진짜 Binary Search!\n\n", avgSearch4, float64(avgSearch4)/float64(avgSearch1))

	fmt.Println("=== 핵심 교훈 ===")
	fmt.Println("1. Linked List: 삽입 O(1)은 빠르지만 검색 O(N)으로 느림")
	fmt.Println("2. Page Buffering: 알고리즘은 같아도 I/O 최적화로 실제 성능 대폭 향상")
	fmt.Println("3. Binary Tree 불균형: 순차 삽입하면 Linked List처럼 퇴화 (높이 = N)")
	fmt.Println("4. Binary Tree 균형: 랜덤 삽입으로 균형잡히면 진짜 O(log N) 성능 발휘")
	fmt.Println("5. 실제 DB 해결책: B-Tree는 자동으로 균형 유지 + 페이지 최적화 = 최고 성능")
	fmt.Printf("\n💡 Binary Tree 균형 유무로 검색 성능 %.0f배 차이!\n", float64(avgSearch3)/float64(avgSearch4))
}
