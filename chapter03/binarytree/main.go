package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// 다른 파일을 잘못 열었을 때 조기 실패를 위한 용도
var Magic = [4]byte{'B', 'S', 'T', 'R'}
var Endian = binary.BigEndian
var ErrInvalidMagic = errors.New("Invalid file: magic mismatch")

const DefaultPageSize uint16 = 4096

// NodeLocation represents position in file
const NullOffset int64 = -1

// 노드의 on-disk 크기
// Value: uint32 (4)
// Left: int64 (8)
// Right: int64 (8)
// Tomb: uint8 (1)
// _pad: [7]byte (7)
const nodeOnDiskSize = 28

// LinkedListStore 인터페이스와 공통 핸들 정의
type LinkedListStore interface {
	Open(path string, truncate bool) (*Handle, error)
	AppendTail(h *Handle, value uint32) error
	DeleteFirstByValue(h *Handle, value uint32) (bool, error)
	TraverseValues(h *Handle) ([]uint32, error)
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

type BinaryTreeStore struct{}

// 파일 헤더
// Magic: 포맷 식별자
// Version: 버전
// PageSize: 추후 확장용
// RootOffset: 루트 노드의 파일 오프셋(없으면 -1)
// Size: 통계 / 검증 용도
type Header struct {
	Magic      [4]byte
	Version    uint16
	PageSize   uint16
	RootOffset int64
	Size       int64
}

func (h *Header) headerVersion() uint16 {
	return h.Version
}

// Binary Tree 노드
type Node struct {
	Value uint32             // 실제 값
	Left  int64              // 왼쪽 자식의 파일 오프셋
	Right int64              // 오른쪽 자식의 파일 오프셋
	Tomb  uint8              // 논리 삭제 마크 (0 == 유효, 1 == 삭제됨)
	_pad  [7]byte            // 28바이트 정렬을 위한 패딩
}

func ensureBinaryTreeHeader(h *Handle) (*Header, error) {
	header, ok := h.Header.(*Header)
	if !ok {
		return nil, fmt.Errorf("binary tree handle does not contain a binary tree header")
	}
	return header, nil
}

func writeHeader(f *os.File, hdr *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, 0, 4+2+2+8+8)
	buf = append(buf, hdr.Magic[:]...)
	buf = Endian.AppendUint16(buf, hdr.Version)
	buf = Endian.AppendUint16(buf, hdr.PageSize)
	buf = Endian.AppendUint64(buf, uint64(hdr.RootOffset))
	buf = Endian.AppendUint64(buf, uint64(hdr.Size))

	_, err := f.Write(buf)
	return err
}

func readHeader(f *os.File, h *Header) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, 4+2+2+8+8)

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
	h.RootOffset = int64(Endian.Uint64(buf[8:16]))
	h.Size = int64(Endian.Uint64(buf[16:24]))

	return nil
}

// 파일 열기/초기화
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
		hdr := &Header{
			Magic:      Magic,
			Version:    1,
			PageSize:   DefaultPageSize,
			RootOffset: NullOffset,
			Size:       0,
		}
		if err := writeHeader(f, hdr); err != nil {
			f.Close()
			return nil, err
		}
	}

	hrd := &Header{}

	if err := readHeader(f, hrd); err != nil {
		f.Close()
		return nil, err
	}

	return &Handle{
		File:   f,
		Header: hrd,
	}, nil
}

func (s *BinaryTreeStore) Close(h *Handle) error {
	return h.File.Close()
}

// 노드 읽기 / 쓰기
func writeNodeAt(f *os.File, off int64, n *Node) error {
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}

	buf := make([]byte, nodeOnDiskSize)

	Endian.PutUint32(buf[0:4], n.Value)
	Endian.PutUint64(buf[4:12], uint64(n.Left))
	Endian.PutUint64(buf[12:20], uint64(n.Right))
	buf[20] = n.Tomb

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
		Left:  int64(Endian.Uint64(buf[4:12])),
		Right: int64(Endian.Uint64(buf[12:20])),
		Tomb:  buf[20],
	}

	return n, nil
}

// Binary Search Tree 삽입 (재귀적)
func insertNode(f *os.File, nodeOffset int64, value uint32) (int64, error) {
	// 노드가 없으면 새로 생성
	if nodeOffset == NullOffset {
		newOffset, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return NullOffset, err
		}

		newNode := &Node{
			Value: value,
			Left:  NullOffset,
			Right: NullOffset,
			Tomb:  0,
		}

		if err := writeNodeAt(f, newOffset, newNode); err != nil {
			return NullOffset, err
		}

		return newOffset, nil
	}

	// 기존 노드 읽기
	node, err := readNodeAt(f, nodeOffset)
	if err != nil {
		return NullOffset, err
	}

	// 값에 따라 왼쪽 또는 오른쪽으로 재귀
	if value < node.Value {
		newLeft, err := insertNode(f, node.Left, value)
		if err != nil {
			return NullOffset, err
		}
		node.Left = newLeft
		if err := writeNodeAt(f, nodeOffset, node); err != nil {
			return NullOffset, err
		}
	} else if value > node.Value {
		newRight, err := insertNode(f, node.Right, value)
		if err != nil {
			return NullOffset, err
		}
		node.Right = newRight
		if err := writeNodeAt(f, nodeOffset, node); err != nil {
			return NullOffset, err
		}
	}
	// value == node.Value 인 경우 중복이므로 무시

	return nodeOffset, nil
}

// AppendTail - Binary Search Tree에서는 BST 삽입으로 구현
// O(log N) ~ O(N) depending on tree balance
func (s *BinaryTreeStore) AppendTail(handle *Handle, value uint32) error {
	h, err := ensureBinaryTreeHeader(handle)
	if err != nil {
		return err
	}
	f := handle.File

	newRoot, err := insertNode(f, h.RootOffset, value)
	if err != nil {
		return err
	}

	h.RootOffset = newRoot
	h.Size++

	return writeHeader(f, h)
}

// In-order traversal (왼쪽 -> 노드 -> 오른쪽)
func inorderTraversal(f *os.File, nodeOffset int64, values *[]uint32) error {
	if nodeOffset == NullOffset {
		return nil
	}

	node, err := readNodeAt(f, nodeOffset)
	if err != nil {
		return err
	}

	// 왼쪽 서브트리
	if err := inorderTraversal(f, node.Left, values); err != nil {
		return err
	}

	// 현재 노드 (삭제되지 않은 경우만)
	if node.Tomb == 0 {
		*values = append(*values, node.Value)
	}

	// 오른쪽 서브트리
	if err := inorderTraversal(f, node.Right, values); err != nil {
		return err
	}

	return nil
}

// TraverseValues - In-order traversal로 정렬된 순서로 반환
// O(N) - 모든 노드 방문
func (s *BinaryTreeStore) TraverseValues(handle *Handle) ([]uint32, error) {
	h, err := ensureBinaryTreeHeader(handle)
	if err != nil {
		return nil, err
	}
	f := handle.File

	values := make([]uint32, 0, h.Size)

	if err := inorderTraversal(f, h.RootOffset, &values); err != nil {
		return nil, err
	}

	return values, nil
}

// Binary Search Tree 탐색
// O(log N) ~ O(N) depending on tree balance
func searchNode(f *os.File, nodeOffset int64, target uint32) (int64, error) {
	if nodeOffset == NullOffset {
		return NullOffset, nil
	}

	node, err := readNodeAt(f, nodeOffset)
	if err != nil {
		return NullOffset, err
	}

	if node.Tomb == 0 && node.Value == target {
		return nodeOffset, nil
	}

	if target < node.Value {
		return searchNode(f, node.Left, target)
	}
	return searchNode(f, node.Right, target)
}

// Where - Binary Search로 탐색
// O(log N) ~ O(N) depending on tree balance
func (s *BinaryTreeStore) Where(handle *Handle, target uint32) (int64, error) {
	h, err := ensureBinaryTreeHeader(handle)
	if err != nil {
		return NullOffset, err
	}
	f := handle.File

	return searchNode(f, h.RootOffset, target)
}

// DeleteFirstByValue - Binary Search Tree에서 논리적 삭제
// O(log N) ~ O(N) depending on tree balance
func (s *BinaryTreeStore) DeleteFirstByValue(handle *Handle, value uint32) (bool, error) {
	h, err := ensureBinaryTreeHeader(handle)
	if err != nil {
		return false, err
	}
	f := handle.File

	offset, err := searchNode(f, h.RootOffset, value)
	if err != nil {
		return false, err
	}

	if offset == NullOffset {
		return false, nil
	}

	node, err := readNodeAt(f, offset)
	if err != nil {
		return false, err
	}

	node.Tomb = 1
	if err := writeNodeAt(f, offset, node); err != nil {
		return false, err
	}

	if h.Size > 0 {
		h.Size--
	}

	return true, writeHeader(f, h)
}

func main() {
	var store LinkedListStore = &BinaryTreeStore{}

	N := 10000
	// 교육용: 항상 새로 시작(O_TRUNC)
	handle, err := store.Open("binary_tree.bst", true)
	if err != nil {
		panic(err)
	}
	defer store.Close(handle)

	fmt.Println("=== Binary Search Tree Implementation ===")
	fmt.Printf("Inserting %d values...\n", N)

	// 순차적 삽입 (최악의 경우 - 불균형 트리)
	for i := 0; i < N; i++ {
		if err := store.AppendTail(handle, uint32(i)); err != nil {
			panic(err)
		}
	}

	fmt.Println("\nSearching for value 9999...")
	offset, err := store.Where(handle, 9999)
	if err != nil {
		panic(err)
	}
	if offset != NullOffset {
		fmt.Printf("Found at offset: %d\n", offset)
	} else {
		fmt.Println("Not found")
	}

	fmt.Println("\nSearching for value 5000...")
	offset, err = store.Where(handle, 5000)
	if err != nil {
		panic(err)
	}
	if offset != NullOffset {
		fmt.Printf("Found at offset: %d\n", offset)
	} else {
		fmt.Println("Not found")
	}

	fmt.Println("\nDeleting value 9999...")
	deleted, err := store.DeleteFirstByValue(handle, 9999)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Deleted: %v\n", deleted)

	fmt.Println("\nSearching for value 9999 after deletion...")
	offset, err = store.Where(handle, 9999)
	if err != nil {
		panic(err)
	}
	if offset != NullOffset {
		fmt.Printf("Found at offset: %d\n", offset)
	} else {
		fmt.Println("Not found (correctly deleted)")
	}

	// Traverse first 10 values
	fmt.Println("\nTraversing first 10 values (in-order)...")
	values, err := store.TraverseValues(handle)
	if err != nil {
		panic(err)
	}
	if len(values) > 10 {
		fmt.Printf("First 10: %v\n", values[:10])
	} else {
		fmt.Printf("All values: %v\n", values)
	}

	fmt.Printf("\nTotal values in tree: %d\n", len(values))
}
