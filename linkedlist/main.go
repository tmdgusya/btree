package main

import "encoding/binary"

// 다른 파일을 잘못 열었을 때 조기 실패를 위한 용도
var Magic = [4]byte{'L', 'L', 'S', 'T'}
var Endian = binary.BigEndian

const DefaultPageSize uint16 = 4096
const NullOffset int64 = -1

// Node 의 on-disk 고정 길이(예: 16바이트) 로 맞추기 위한 패딩 크기
const nodePadBytes = 3
