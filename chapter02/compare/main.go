package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

//
// =======================================
// 공통: I/O 계측용 래퍼
// =======================================
//

// IOMetrics 는 파일에 대해 얼마나 많은 I/O 호출이 있었는지 저장한다.
// - Reads: Read 호출 횟수
// - Writes: Write 호출 횟수
// - Seeks: Seek 호출 횟수
type IOMetrics struct {
	Reads  int64
	Writes int64
	Seeks  int64
}

// CountingFile 은 os.File 위에 얇은 래퍼를 씌워서
// Read/Write/Seek 호출 횟수를 카운트한다.
// 실제 디스크 I/O를 완벽히 반영하는 것은 아니지만,
// "코드가 얼마나 자주 파일 포인터를 움직이고 읽고 쓰는지"를 비교하는 데엔 충분하다.
type CountingFile struct {
	f  *os.File
	Io *IOMetrics
}

// NewCountingFile 는 주어진 os.File 을 감싸는 CountingFile 을 만든다.
func NewCountingFile(f *os.File) *CountingFile {
	return &CountingFile{
		f:  f,
		Io: &IOMetrics{},
	}
}

// Read 는 내부 파일의 Read를 호출하고, 호출 횟수를 1 증가시킨다.
func (cf *CountingFile) Read(p []byte) (int, error) {
	cf.Io.Reads++
	return cf.f.Read(p)
}

// Write 는 내부 파일의 Write를 호출하고, 호출 횟수를 1 증가시킨다.
func (cf *CountingFile) Write(p []byte) (int, error) {
	cf.Io.Writes++
	return cf.f.Write(p)
}

// Seek 는 내부 파일의 Seek를 호출하고, 호출 횟수를 1 증가시킨다.
func (cf *CountingFile) Seek(offset int64, whence int) (int64, error) {
	cf.Io.Seeks++
	return cf.f.Seek(offset, whence)
}

// Close 는 내부 파일을 닫는다.
func (cf *CountingFile) Close() error {
	return cf.f.Close()
}

// ReadFull 은 io.ReadFull 을 감싸되, CountingFile.Read 를 통해 읽게 해서
// Read 호출 횟수가 잘 집계되도록 한다.
func ReadFull(r io.Reader, buf []byte) (int, error) {
	// io.ReadFull 이 내부에서 Read 를 여러 번 부를 수 있으므로
	// CountingFile.Read 가 몇 번 불렸는지 그대로 누적된다.
	return io.ReadFull(r, buf)
}

//
// =======================================
// 1단계: 오프셋 기반 LinkedList (단순 버전)
// =======================================
//

// 직렬화에 사용할 엔디안. 시리즈 일관성을 위해 BigEndian 사용.
var Endian = binary.BigEndian

// 오프셋 기반 리스트의 헤더.
// - Magic: 포맷 식별("LLOF" = Linked List, OFfset 기반)
// - Version: 버전
// - HeadOffset/TailOffset: 첫/마지막 노드의 파일 오프셋
// - Size: 노드 개수
type OffsetHeader struct {
	Magic      [4]byte
	Version    uint16
	_          uint16 // 패딩 (정렬 맞추기용, 의미 없음)
	HeadOffset int64
	TailOffset int64
	Size       int64
}

// 오프셋 기반 노드.
// - Value: 실제 값
// - Next: 다음 노드의 파일 오프셋(없으면 -1)
type OffsetNode struct {
	Value uint32
	Next  int64
}

var OffsetMagic = [4]byte{'L', 'L', 'O', 'F'}

const NullOffset int64 = -1

const offsetHeaderSize = 4 + 2 + 2 + 8 + 8 + 8
const offsetNodeSize = 4 + 8

// Offset 리스트용 헤더 쓰기/읽기
func writeOffsetHeader(f *CountingFile, h *OffsetHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, offsetHeaderSize)
	copy(buf[0:4], h.Magic[:])
	Endian.PutUint16(buf[4:6], h.Version)
	// buf[6:8] 패딩은 0
	Endian.PutUint64(buf[8:16], uint64(h.HeadOffset))
	Endian.PutUint64(buf[16:24], uint64(h.TailOffset))
	Endian.PutUint64(buf[24:32], uint64(h.Size))
	_, err := f.Write(buf)
	return err
}

func readOffsetHeader(f *CountingFile, h *OffsetHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, offsetHeaderSize)
	if _, err := ReadFull(f, buf); err != nil {
		return err
	}
	copy(h.Magic[:], buf[0:4])
	h.Version = Endian.Uint16(buf[4:6])
	h.HeadOffset = int64(Endian.Uint64(buf[8:16]))
	h.TailOffset = int64(Endian.Uint64(buf[16:24]))
	h.Size = int64(Endian.Uint64(buf[24:32]))
	return nil
}

// 특정 오프셋에 OffsetNode 쓰기/읽기
func writeOffsetNodeAt(f *CountingFile, off int64, n *OffsetNode) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, offsetNodeSize)
	Endian.PutUint32(buf[0:4], n.Value)
	Endian.PutUint64(buf[4:12], uint64(n.Next))
	_, err := f.Write(buf)
	return err
}

func readOffsetNodeAt(f *CountingFile, off int64) (OffsetNode, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return OffsetNode{}, err
	}
	buf := make([]byte, offsetNodeSize)
	if _, err := ReadFull(f, buf); err != nil {
		return OffsetNode{}, err
	}
	var n OffsetNode
	n.Value = Endian.Uint32(buf[0:4])
	n.Next = int64(Endian.Uint64(buf[4:12]))
	return n, nil
}

// Offset 리스트 초기화
func OpenOffsetList(path string, truncate bool) (*CountingFile, *OffsetHeader, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}
	raw, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, nil, err
	}
	f := NewCountingFile(raw)

	info, err := raw.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if info.Size() == 0 || truncate {
		h := &OffsetHeader{
			Magic:      OffsetMagic,
			Version:    1,
			HeadOffset: NullOffset,
			TailOffset: NullOffset,
			Size:       0,
		}
		if err := writeOffsetHeader(f, h); err != nil {
			f.Close()
			return nil, nil, err
		}
		return f, h, nil
	}

	h := &OffsetHeader{}
	if err := readOffsetHeader(f, h); err != nil {
		f.Close()
		return nil, nil, err
	}
	if h.Magic != OffsetMagic {
		f.Close()
		return nil, nil, fmt.Errorf("offset: magic mismatch")
	}
	return f, h, nil
}

// Offset 리스트: 꼬리에 추가
func offsetAppendTail(f *CountingFile, h *OffsetHeader, value uint32) error {
	// 파일 끝에 새 노드 기록
	newNode := &OffsetNode{Value: value, Next: NullOffset}
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

	// 기존 tail 의 Next 를 새 노드 위치로 갱신 (랜덤 쓰기 1회)
	tailNode, err := readOffsetNodeAt(f, h.TailOffset)
	if err != nil {
		return err
	}
	tailNode.Next = newOff
	if err := writeOffsetNodeAt(f, h.TailOffset, &tailNode); err != nil {
		return err
	}

	h.TailOffset = newOff
	h.Size++
	return writeOffsetHeader(f, h)
}

// Offset 리스트: 머리에 추가
func offsetPrependHead(f *CountingFile, h *OffsetHeader, value uint32) error {
	newNode := &OffsetNode{Value: value, Next: h.HeadOffset}
	newOff, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if err := writeOffsetNodeAt(f, newOff, newNode); err != nil {
		return err
	}

	if h.HeadOffset == NullOffset {
		h.TailOffset = newOff
	}
	h.HeadOffset = newOff
	h.Size++
	return writeOffsetHeader(f, h)
}

// Offset 리스트: 순회
func offsetTraverseValues(f *CountingFile, h *OffsetHeader) ([]uint32, error) {
	out := make([]uint32, 0, h.Size)
	off := h.HeadOffset
	for off != NullOffset {
		n, err := readOffsetNodeAt(f, off)
		if err != nil {
			return nil, err
		}
		out = append(out, n.Value)
		off = n.Next
	}
	return out, nil
}

//
// =======================================
// 2단계: 페이지/슬롯 기반 LinkedList (단순 버전)
// =======================================
//

// 페이지/슬롯 기반 헤더.
// - Magic: "LLPG" (Linked List, PaGed)
// - Version: 2
// - PageSize: 페이지 크기(4096)
// - PageCount: 현재 페이지 개수
// - HeadPage/HeadSlot, TailPage/TailSlot: 머리/꼬리 노드 위치
// - Size: 노드 개수
type PagedHeader struct {
	Magic     [4]byte
	Version   uint16
	PageSize  uint16
	PageCount uint32
	HeadPage  uint32
	HeadSlot  uint16
	TailPage  uint32
	TailSlot  uint16
	Size      int64
}

var PagedMagic = [4]byte{'L', 'L', 'P', 'G'}

const PAGE_SIZE = 4096
const PAGE_HEADER_SIZE = 2  // Used(uint16)
const SLOT_SIZE = 4 + 4 + 2 // Value(uint32) + NextPage(uint32) + NextSlot(uint16)
const SLOTS_PER_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE

// "없음"을 나타내기 위한 특수 페이지/슬롯 값
const NullPage uint32 = ^uint32(0) // 0xFFFFFFFF
const NullSlot uint16 = ^uint16(0) // 0xFFFF

// 페이지 헤더
type PageHeader struct {
	Used uint16
}

// 페이지/슬롯 기반 노드
type PagedNode struct {
	Value    uint32
	NextPage uint32
	NextSlot uint16
}

const pagedHeaderSize = 4 + 2 + 2 + 4 + 4 + 2 + 4 + 2 + 8
const OFFSET_PAGE_SIZE = 4096

// 헤더 쓰기/읽기
func writePagedHeader(f *CountingFile, h *PagedHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, pagedHeaderSize)
	copy(buf[0:4], h.Magic[:])
	Endian.PutUint16(buf[4:6], h.Version)
	Endian.PutUint16(buf[6:8], h.PageSize)
	Endian.PutUint32(buf[8:12], h.PageCount)
	Endian.PutUint32(buf[12:16], h.HeadPage)
	Endian.PutUint16(buf[16:18], h.HeadSlot)
	Endian.PutUint32(buf[18:22], h.TailPage)
	Endian.PutUint16(buf[22:24], h.TailSlot)
	Endian.PutUint64(buf[24:32], uint64(h.Size))
	_, err := f.Write(buf)
	return err
}

func readPagedHeader(f *CountingFile, h *PagedHeader) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, pagedHeaderSize)
	if _, err := ReadFull(f, buf); err != nil {
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
	h.Size = int64(Endian.Uint64(buf[24:32]))
	return nil
}

// 페이지 시작 오프셋 계산 (헤더 뒤에 페이지들이 붙음)
func pageOffset(pageID uint32) int64 {
	return int64(pagedHeaderSize) + int64(pageID)*PAGE_SIZE
}

// 새 페이지 초기화 (Used=0, 나머지 0)
func initEmptyPage(f *CountingFile, pageID uint32) error {
	off := pageOffset(pageID)
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, PAGE_SIZE) // 전부 0
	_, err := f.Write(buf)
	return err
}

// 페이지 헤더 읽기/쓰기
func readPageHeader(f *CountingFile, pageID uint32) (PageHeader, error) {
	off := pageOffset(pageID)
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return PageHeader{}, err
	}
	buf := make([]byte, PAGE_HEADER_SIZE)
	if _, err := ReadFull(f, buf); err != nil {
		return PageHeader{}, err
	}
	var ph PageHeader
	ph.Used = Endian.Uint16(buf[0:2])
	return ph, nil
}

func writePageHeader(f *CountingFile, pageID uint32, ph *PageHeader) error {
	off := pageOffset(pageID)
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, PAGE_HEADER_SIZE)
	Endian.PutUint16(buf[0:2], ph.Used)
	_, err := f.Write(buf)
	return err
}

// 특정 페이지/슬롯 위치에 노드 쓰기/읽기
func writeSlot(f *CountingFile, pageID uint32, slot uint16, n *PagedNode) error {
	off := pageOffset(pageID) + int64(PAGE_HEADER_SIZE) + int64(slot)*SLOT_SIZE
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, SLOT_SIZE)
	Endian.PutUint32(buf[0:4], n.Value)
	Endian.PutUint32(buf[4:8], n.NextPage)
	Endian.PutUint16(buf[8:10], n.NextSlot)
	_, err := f.Write(buf)
	return err
}

func readSlot(f *CountingFile, pageID uint32, slot uint16) (PagedNode, error) {
	off := pageOffset(pageID) + int64(PAGE_HEADER_SIZE) + int64(slot)*SLOT_SIZE
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return PagedNode{}, err
	}
	buf := make([]byte, SLOT_SIZE)
	if _, err := ReadFull(f, buf); err != nil {
		return PagedNode{}, err
	}
	var n PagedNode
	n.Value = Endian.Uint32(buf[0:4])
	n.NextPage = Endian.Uint32(buf[4:8])
	n.NextSlot = Endian.Uint16(buf[8:10])
	return n, nil
}

// 새로운 슬롯 하나를 할당한다.
// - 마지막 페이지에 빈 슬롯이 있으면 그대로 사용.
// - 다 찼으면 새 페이지를 만들고 그 페이지의 0번 슬롯 사용.
func allocateSlot(f *CountingFile, h *PagedHeader) (pageID uint32, slot uint16, err error) {
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

	slot = ph.Used
	ph.Used++
	if err = writePageHeader(f, pageID, &ph); err != nil {
		return
	}
	return
}

// Paged 리스트 초기화
func OpenPagedList(path string, truncate bool) (*CountingFile, *PagedHeader, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}
	raw, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, nil, err
	}
	f := NewCountingFile(raw)

	info, err := raw.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	if info.Size() == 0 || truncate {
		h := &PagedHeader{
			Magic:     PagedMagic,
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
			return nil, nil, err
		}
		return f, h, nil
	}

	h := &PagedHeader{}
	if err := readPagedHeader(f, h); err != nil {
		f.Close()
		return nil, nil, err
	}
	if h.Magic != PagedMagic {
		f.Close()
		return nil, nil, fmt.Errorf("paged: magic mismatch")
	}
	return f, h, nil
}

// Paged 리스트: 꼬리에 추가
func pagedAppendTail(f *CountingFile, h *PagedHeader, value uint32) error {
	pageID, slot, err := allocateSlot(f, h)
	if err != nil {
		return err
	}

	newNode := &PagedNode{
		Value:    value,
		NextPage: NullPage,
		NextSlot: NullSlot,
	}
	if err := writeSlot(f, pageID, slot, newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.HeadPage = pageID
		h.HeadSlot = slot
		h.TailPage = pageID
		h.TailSlot = slot
		h.Size++
		return writePagedHeader(f, h)
	}

	tailNode, err := readSlot(f, h.TailPage, h.TailSlot)
	if err != nil {
		return err
	}
	tailNode.NextPage = pageID
	tailNode.NextSlot = slot
	if err := writeSlot(f, h.TailPage, h.TailSlot, &tailNode); err != nil {
		return err
	}

	h.TailPage = pageID
	h.TailSlot = slot
	h.Size++
	return writePagedHeader(f, h)
}

// Paged 리스트: 머리에 추가
func pagedPrependHead(f *CountingFile, h *PagedHeader, value uint32) error {
	pageID, slot, err := allocateSlot(f, h)
	if err != nil {
		return err
	}

	newNode := &PagedNode{
		Value:    value,
		NextPage: h.HeadPage,
		NextSlot: h.HeadSlot,
	}
	if err := writeSlot(f, pageID, slot, newNode); err != nil {
		return err
	}

	if h.HeadPage == NullPage {
		h.TailPage = pageID
		h.TailSlot = slot
	}
	h.HeadPage = pageID
	h.HeadSlot = slot
	h.Size++
	return writePagedHeader(f, h)
}

// Paged 리스트: 순회
func pagedTraverseValues(f *CountingFile, h *PagedHeader) ([]uint32, error) {
	out := make([]uint32, 0, h.Size)
	page := h.HeadPage
	slot := h.HeadSlot
	for page != NullPage && slot != NullSlot {
		n, err := readSlot(f, page, slot)
		if err != nil {
			return nil, err
		}
		out = append(out, n.Value)
		page = n.NextPage
		slot = n.NextSlot
	}
	return out, nil
}

// offsetTraverseWithPageStats
// - Offset 기반 리스트를 head부터 끝까지 순회하면서
//  1. 값들 (values)
//  2. 각 페이지가 몇 번 방문됐는지(pageVisits[pageID] = count)
//     를 함께 반환한다.
//
// 여기서 "페이지"는 OFFSET_PAGE_SIZE(4KB) 단위로 나눈 가상의 페이지 개념이다.
// 즉, 노드가 위치한 오프셋을 4KB 로 나눠서 pageID 를 계산한다.
func offsetTraverseWithPageStats(f *CountingFile, h *OffsetHeader) ([]uint32, map[uint32]int, error) {
	values := make([]uint32, 0, h.Size)
	pageVisits := make(map[uint32]int)

	off := h.HeadOffset
	for off != NullOffset {
		n, err := readOffsetNodeAt(f, off)
		if err != nil {
			return nil, nil, err
		}

		values = append(values, n.Value)

		// 현재 노드가 위치한 "페이지 번호" 계산
		pageID := uint32(off / OFFSET_PAGE_SIZE)
		pageVisits[pageID]++

		off = n.Next
	}
	return values, pageVisits, nil
}

// pagedTraverseWithPageStats
// - 페이지/슬롯 기반 리스트를 head부터 끝까지 순회하면서
//  1. 값들 (values)
//  2. 각 페이지ID 가 몇 번 방문됐는지(pageVisits[pageID] = count)
//     를 함께 반환한다.
//
// 여기서 페이지는 실제 pageID (0,1,2,...) 를 그대로 쓴다.
// 이미 노드 주소가 (PageID, SlotID) 로 되어 있기 때문.
func pagedTraverseWithPageStats(f *CountingFile, h *PagedHeader) ([]uint32, map[uint32]int, error) {
	values := make([]uint32, 0, h.Size)
	pageVisits := make(map[uint32]int)

	for pageID := uint32(0); pageID < h.PageCount; pageID++ {
		ph, err := readPageHeader(f, pageID)
		if err != nil {
			return nil, pageVisits, err
		}

		for slotID := uint16(0); slotID < ph.Used; slotID++ {
			_, err := readSlot(f, pageID, slotID)
			if err != nil {
				return nil, pageVisits, err
			}
		}
	}

	return values, pageVisits, nil
}

// =======================================
// 비교용 메인 함수
// =======================================
func main() {
	const N = 100000

	// ---------------------------
	// 1) Offset 기반 리스트
	// ---------------------------
	offsetFile, offsetHdr, err := OpenOffsetList("offset_list.llst", true)
	if err != nil {
		panic(err)
	}
	defer offsetFile.Close()

	for i := 100; i < N; i++ {
		if err := offsetAppendTail(offsetFile, offsetHdr, uint32(i)); err != nil {
			panic(err)
		}
	}

	offsetVals, offsetPages, err := offsetTraverseWithPageStats(offsetFile, offsetHdr)
	if err != nil {
		panic(err)
	}
	fmt.Println("Offset list length:", len(offsetVals))

	// ---------------------------
	// 2) Paged 기반 리스트
	// ---------------------------
	pagedFile, pagedHdr, err := OpenPagedList("paged_list.llst", true)
	if err != nil {
		panic(err)
	}
	defer pagedFile.Close()

	for i := 100; i < N; i++ {
		if err := pagedAppendTail(pagedFile, pagedHdr, uint32(i)); err != nil {
			panic(err)
		}
	}

	pagedVals, pagedPages, err := pagedTraverseWithPageStats(pagedFile, pagedHdr)
	if err != nil {
		panic(err)
	}
	fmt.Println("Paged list length :", len(pagedVals))

	// ---------------------------
	// 3) I/O Metrics + 페이지 통계 비교
	// ---------------------------
	fmt.Println("==== I/O Metrics ====")
	fmt.Printf("[Offset] Reads=%d, Writes=%d, Seeks=%d\n",
		offsetFile.Io.Reads, offsetFile.Io.Writes, offsetFile.Io.Seeks)
	fmt.Printf("[Paged ] Reads=%d, Writes=%d, Seeks=%d\n",
		pagedFile.Io.Reads, pagedFile.Io.Writes, pagedFile.Io.Seeks)

	fmt.Println("==== Page Stats (unique pages touched during traversal) ====")
	fmt.Printf("[Offset] Unique Pages = %d\n", len(offsetPages))
	fmt.Printf("[Paged ] Unique Pages = %d\n", len(pagedPages))
}
