package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// 다른 파일을 잘못 열었을 때 조기 실패를 위한 용도
var Magic = [4]byte{'L', 'L', 'S', 'T'}
var Endian = binary.BigEndian
var ErrInvalidMagic = errors.New("Invalid file: magic mismatch")

const DefaultPageSize uint16 = 4096
const NullOffset int64 = -1

// Node 의 on-disk 고정 길이(예: 16바이트) 로 맞추기 위한 패딩 크기
const nodePadBytes = 3

// 구조체
// 파일 헤
// Magic: 포맷 식별자
// Version: 버전
// PageSize: 추후에 페이지네이션으로 업그레이드 할때 이용
// HeadOffset: 첫 노드의 파일 오프셋(없으면 -1)
// TailOffset: 마지막 노드의 파일 오프셋(없으면 -1)
// Size: 통계 / 검증 용도
// FreeList: 삭제된 노드들의 단일 연결리스트 머리
type Header struct {
	Magic      [4]byte
	Version    uint16
	PageSize   uint16
	HeadOffset int64
	TailOffset int64
	Size       int64
	FreeList   int64
}

// LinkedList 노드
// - Value: 실제 값(32비트 정수; 예제 단순화를 위해 uint32 이용)
// - Next: 다음 노드의 파일 오프셋 (없으면 -1)
// - Tomb: 논리 삭제 마크 (0 == 유효, 1 == 삭제됨). 물리 삭제는 하지 않음
// - _pad: 16 바이트 정렬을 위해 3바이트 패딩 (읽기 쉬운 고정 길이 유지)
type Node struct {
	Value uint32
	Next  int64
	Tomb  uint8
	_pad  [nodePadBytes]byte
}

func writeHeader(f *os.File, hdr *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, 0, 4+2+2+8+8+8+8)
	buf = append(buf, hdr.Magic[:]...)
	buf = Endian.AppendUint16(buf, hdr.Version)
	buf = Endian.AppendUint16(buf, hdr.PageSize)
	buf = Endian.AppendUint64(buf, uint64(hdr.HeadOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.TailOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.Size))
	buf = Endian.AppendUint64(buf, uint64(hdr.FreeList))

	_, err := f.Write(buf)
	return err
}

// 파일 열기/초기화
func Open(path string, truncate bool) (*os.File, *Header, error) {
	flags := os.O_RDWR | os.O_CREATE
	if truncate {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}

	if info.Size() == 0 || truncate {
		hdr := &Header{
			Magic:      Magic,
			Version:    1,
			PageSize:   DefaultPageSize,
			HeadOffset: NullOffset,
			TailOffset: NullOffset,
			Size:       0,
			FreeList:   NullOffset,
		}
		if err := writeHeader(f, hdr); err != nil {
			return nil, nil, err
		}
	}

	hrd := &Header{}

	if err := readHeader(f, hrd); err != nil {
		f.Close()
		return nil, nil, err
	}

	return f, hrd, nil
}

func readHeader(f *os.File, h *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, 4+2+2+8+8+8+8)

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
	h.HeadOffset = int64(Endian.Uint64(buf[8:16]))
	h.TailOffset = int64(Endian.Uint64(buf[16:24]))
	h.Size = int64(Endian.Uint64(buf[24:32]))
	h.FreeList = int64(Endian.Uint64(buf[32:40]))

	return nil
}

// 노드 읽기 / 쓰기 (고정 16 바이트)

const nodeOnDiskSize = 4 + 8 + 1 + nodePadBytes

func writeNodeAt(f *os.File, off int64, n *Node) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, nodeOnDiskSize)

	Endian.PutUint32(buf[0:4], uint32(n.Value))
	Endian.PutUint64(buf[4:12], uint64(n.Next))
	buf[12] = byte(n.Tomb)

	if _, err := f.Write(buf); err != nil {
		return err
	}

	return nil
}

func readNodeAt(f *os.File, off int64) (*Node, error) {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}

	buf := make([]byte, nodeOnDiskSize)

	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}

	n := &Node{
		Value: Endian.Uint32(buf[0:4]),
		Next:  int64(Endian.Uint64(buf[4:12])),
		Tomb:  buf[12],
	}

	return n, nil
}

// 리스트 연산
func appendTail(f *os.File, h *Header, value uint32) error {
	newNode := &Node{
		Value: value,
		Next:  NullOffset,
		Tomb:  0,
	}

	newOff, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := writeNodeAt(f, newOff, newNode); err != nil {
		return err
	}

	if h.HeadOffset == NullOffset {
		h.HeadOffset = newOff
		h.TailOffset = newOff
		h.Size++
		return writeHeader(f, h)
	}

	// 기존 tail 노드의 Next 를 새 노드의 Next 로 설정
	tailNode, err := readNodeAt(f, h.TailOffset)
	if err != nil {
		return err
	}

	tailNode.Next = newOff
	if err := writeNodeAt(f, h.TailOffset, tailNode); err != nil {
		return err
	}

	h.TailOffset = newOff
	h.Size++

	return writeHeader(f, h)
}

// 리스트 연산
func prependHead(f *os.File, h *Header, value uint32) error {
	newNode := &Node{
		Value: value,
		Next:  h.HeadOffset,
		Tomb:  0,
	}

	newOff, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if err := writeNodeAt(f, newOff, newNode); err != nil {
		return err
	}

	if h.HeadOffset == NullOffset {
		h.TailOffset = newOff
	}

	h.HeadOffset = newOff
	h.Size++

	return writeHeader(f, h)
}

func deleteFirstByValue(f *os.File, h *Header, value uint32) (bool, error) {
	if h.HeadOffset == NullOffset {
		return false, nil
	}

	var prevOff int64 = NullOffset
	var off int64 = h.HeadOffset

	for off != NullOffset {
		node, err := readNodeAt(f, off)
		if err != nil {
			return false, err
		}

		if node.Value == value && node.Tomb == 0 {
			// 원래 Next 값을 저장
			originalNext := node.Next

			node.Tomb = 1
			node.Next = h.FreeList
			if err := writeNodeAt(f, off, node); err != nil {
				return false, err
			}
			h.FreeList = off

			if prevOff == NullOffset {
				// head 가 지워지는 경우
				h.HeadOffset = originalNext
				if h.HeadOffset == NullOffset {
					h.TailOffset = NullOffset
				}
			} else {
				prevNode, err := readNodeAt(f, prevOff)
				if err != nil {
					return false, err
				}
				prevNode.Next = originalNext
				if err := writeNodeAt(f, prevOff, prevNode); err != nil {
					return false, err
				}

				if off == h.TailOffset {
					h.TailOffset = prevOff
				}
			}

			if h.Size > 0 {
				h.Size--
			}

			if err := writeHeader(f, h); err != nil {
				return false, err
			}

			return true, nil
		}

		prevOff = off
		off = node.Next
	}

	return false, nil
}

func traverseValues(f *os.File, h *Header) ([]uint32, error) {
	out := make([]uint32, 0, h.Size)
	off := h.HeadOffset

	for off != NullOffset {
		node, err := readNodeAt(f, off)
		if err != nil {
			return nil, err
		}
		if node.Tomb == 0 {
			out = append(out, node.Value)
		}
		off = node.Next
	}
	return out, nil
}

func main() {
	// 교육용: 항상 새로 시작(O_TRUNC)
	f, hdr, err := Open("linked_list.db", true)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 머리/꼬리 섞어서 삽입해보자(랜덤 I/O 상황 만들기)
	// list: [H] 2 -> 1 -> 0   (prepend 2,1,0)
	if err := prependHead(f, hdr, 0); err != nil {
		panic(err)
	}
	if err := prependHead(f, hdr, 1); err != nil {
		panic(err)
	}
	if err := prependHead(f, hdr, 2); err != nil {
		panic(err)
	}

	// 꼬리에 추가 x3
	// list: 2 -> 1 -> 0 -> 3 -> 4 -> 5
	if err := appendTail(f, hdr, 3); err != nil {
		panic(err)
	}
	if err := appendTail(f, hdr, 4); err != nil {
		panic(err)
	}
	if err := appendTail(f, hdr, 5); err != nil {
		panic(err)
	}

	// 순회 출력(삭제 전)
	vals, err := traverseValues(f, hdr)
	if err != nil {
		panic(err)
	}
	fmt.Println("before delete:", vals) // 기대: [2 1 0 3 4 5]

	// 값 3을 논리 삭제(첫 매칭만)
	found, err := deleteFirstByValue(f, hdr, 3)
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted 3? ->", found)

	// 순회 출력(삭제 후)
	vals, err = traverseValues(f, hdr)
	if err != nil {
		panic(err)
	}
	fmt.Println("after delete :", vals) // 기대: [2 1 0 4 5]

	// 헤더를 다시 읽어 확인(파일 재오픈 상황 흉내)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		panic(err)
	}
	if err := readHeader(f, hdr); err != nil {
		panic(err)
	}
	fmt.Printf("Header{Head=%d, Tail=%d, Size=%d, FreeList=%d}\n",
		hdr.HeadOffset, hdr.TailOffset, hdr.Size, hdr.FreeList)
}
