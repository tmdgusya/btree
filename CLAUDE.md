# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a B-Tree implementation and educational project written in Go. The repository contains:
- A web-based B-Tree visualization server (root `main.go`)
- Educational chapter-based examples demonstrating storage concepts from basic file I/O to page-based linked lists
- Implementations of different storage strategies for understanding database internals

## Development Commands

### Running the Main Application
```bash
go run main.go
```
This starts a web server on port 8080 with an interactive B-Tree visualization interface in Korean (한국어).

### Running Chapter Examples
```bash
# Chapter 1: Basic page and file operations
go run chapter01/page/main.go
go run chapter01/file/main.go

# Chapter 2: Linked list implementations
go run chapter02/linkedlist/main.go          # Offset-based linked list
go run chapter02/paged_linked_list/main.go   # Page-slotted linked list
go run chapter02/compare/main.go             # I/O performance comparison (paged vs naive)

# Chapter 3: Binary tree and comprehensive comparison
go run chapter03/binarytree/main.go          # Binary Search Tree
go run chapter03/compare_all/main.go         # Comprehensive comparison (all 3 structures)
```

### Testing
```bash
# Run all tests
go test ./...

# Run tests in a specific directory
go test ./chapter01/...
go test ./chapter02/...
```

### Building
```bash
go build -o btree main.go
./btree
```

## Architecture

### Main B-Tree Server (`main.go`)

The root application is a web server providing interactive B-Tree visualization:

**Core Data Structures:**
- `BTreeNode`: Keys ([]int), children ([]*BTreeNode), and leaf status
- `BTree`: Root node pointer and degree parameter `t`

**Key Operations:**
- `Insert(k int)`: Insertion with automatic node splitting when capacity (2t-1 keys) is reached
- `Search(k int)`: Returns node and index if found
- `SearchPath(k int)`: Returns traversal path with node labels for visualization

**HTTP API Endpoints:**
- `POST /api/create`: Initialize tree with specified degree `t`
- `POST /api/insert`: Insert value into tree
- `POST /api/search`: Search for value, returns path and highlight info
- `GET /api/state`: Get current tree state as JSON
- `GET /`: Serves embedded HTML/CSS/JS visualization UI

**Thread Safety:** Uses `sync.RWMutex` (treeMu) to protect concurrent access to the in-memory tree.

### Chapter 1: Foundational Storage Concepts

**`chapter01/page/main.go`**: Basic page-based storage system
- `Pager`: Manages fixed-size pages (4096 bytes) with read/write operations
- `Page`: Container with ID and data buffer
- Demonstrates serialization of integer arrays to/from disk using `binary.BigEndian`

**`chapter01/file/main.go`**: Page management with multiple page reading
- `PageManager`: Manages collection of pages with `ReadAt(id)` and `ReadAll()` operations
- Smaller page size (16 bytes) for demonstration

### Chapter 2: Linked List Storage Strategies

**Offset-Based Storage (`chapter02/linkedlist/main.go`)**

**Key Concepts:**
- Each node stored at a file offset
- Header tracks head/tail offsets and size
- Nodes contain file offsets pointing to next nodes

**Data Structures:**
- `Header`: Magic bytes, version, page size, head/tail offsets, size
- `Node`: Value (uint32), Next offset (int64), Tomb flag (uint8), padding
- `OffsetStore`: Implements `LinkedListStore` interface

**Key Methods:**
- `Open(path, truncate)`: Opens/creates file with header initialization
- `AppendTail(handle, value)`: Appends to end, updates tail pointer
- `DeleteFirstByValue(handle, value)`: Logical deletion (tomb flag), relinks chain
- `TraverseValues(handle)`: Follows offsets to collect all live values
- `Where(handle, target)`: Searches for value, returns file offset if found

**Constants:**
- `NullOffset = -1`: Sentinel for null/missing pointers
- Node size: 16 bytes (4 + 8 + 1 + 3 padding)

**Page-Slotted Storage (`chapter02/paged_linked_list/main.go`)**

**Key Concepts:**
- Fixed-size pages (4096 bytes) containing multiple slots
- Each page has a header tracking used slots
- Nodes reference next node via (PageID, SlotID) pairs instead of raw offsets

**Data Structures:**
- `Header`: Similar to offset version but tracks page/slot pairs for head/tail
- `PageHeader`: Tracks how many slots are used in this page (2 bytes)
- `Node`: Value (uint32), NextPage (uint32), NextSlot (uint16), Tomb (uint8), padding (uint8)
- `PagedStore`: Implements `LinkedListStore` interface
- `PageBuffer`: Caches single page to reduce I/O during traversals

**Key Methods:**
- `allocateSlot(f, header)`: Finds free slot in last page or allocates new page
- `AppendTail(handle, value)`: Allocates slot, writes node, updates tail
- `PrependHead(handle, value)`: Inserts at head (demonstrates insertion flexibility)
- `TraverseValues(handle)`: Follows page/slot chain with page buffer optimization
- `TraverseValuesPhysical(handle)`: Scans all pages sequentially (non-logical order)
- `Where(handle, target)`: Returns `*Location{Page, Slot}` if found
- `readSlotWithBuffer`: Loads entire page once, parses slots from memory

**Constants:**
- `PAGE_SIZE = 4096`
- `PAGE_HEADER_SIZE = 2`
- `SLOT_SIZE = 12` (4 + 4 + 2 + 1 + 1)
- `SLOTS_PER_PAGE = 341` ((4096 - 2) / 12)
- `NullPage = 0xFFFFFFFF`, `NullSlot = 0xFFFF`

**Helper Functions:**
- `pageOffset(pageID)`: Calculates file offset for page
- `initEmptyPage(f, pageID)`: Writes zeroed page
- `readPageHeader/writePageHeader`: Page-level metadata I/O
- `readSlot/writeSlot`: Slot-level node I/O

### Chapter 3: Binary Tree and Performance Comparison

**Binary Search Tree Storage (`chapter03/binarytree/main.go`)**

**Key Concepts:**
- Tree-based structure with nodes stored at file offsets
- Each node has left and right child pointers (file offsets)
- Binary search property: left child < parent < right child
- Demonstrates importance of balanced trees for performance

**Data Structures:**
- `Header`: Magic bytes, version, page size, root offset, size
- `Node`: Value (uint32), Left offset (int64), Right offset (int64), Tomb flag (uint8), padding (7 bytes)
- `BinaryTreeStore`: Implements `LinkedListStore` interface

**Key Methods:**
- `Open(path, truncate)`: Opens/creates file with header initialization
- `AppendTail(handle, value)`: Inserts using BST insertion (recursive)
- `Where(handle, target)`: Binary search traversal following left/right pointers
- `DeleteFirstByValue(handle, value)`: Logical deletion (tomb flag)
- `TraverseValues(handle)`: In-order traversal returns sorted values

**Time Complexity:**
- **Balanced Tree**: Insert O(log N), Search O(log N)
- **Unbalanced Tree** (sequential insertion): Insert O(N), Search O(N)
- Demonstrates why real databases use self-balancing trees (B-Tree, AVL, Red-Black)

**Constants:**
- `NullOffset = -1`: Sentinel for null/missing pointers
- Node size: 28 bytes (4 + 8 + 8 + 1 + 7 padding)

**Performance Comparison (`chapter02/compare/main.go`)**

**Purpose:** Demonstrates I/O efficiency differences between naive and buffered traversal in page-slotted storage.

**Key Components:**
- `CountingFile`: Wraps `*os.File` to track Read/Write/Seek operations
- `IOMetrics`: Counters for I/O operations
- `traverseNaive`: Reads each slot individually (Seek + Read per node)
- `traverseBuffered`: Uses `PageBuffer` to read pages once and parse in memory

**Typical Results (N=100,000):**
- Naive: ~200,000 Reads/Seeks (2 per node: read slot, read next)
- Buffered: ~300 Reads/Seeks (one per page, ~341 nodes per page)
- Demonstrates ~650x reduction in I/O operations

**Comprehensive Comparison (`chapter03/compare_all/main.go`)**

**Purpose:** Compares time complexity differences across all three data structure implementations.

**Comparison Results (N=10,000, 1,000 searches):**

| Data Structure | Insert Time | Insert Complexity | Search Time (avg) | Search Complexity |
|---------------|-------------|-------------------|-------------------|-------------------|
| Offset-Based LL | ~42ms | O(1) | ~3.3ms | O(N) |
| Page-Slotted LL | ~50ms | O(1) | ~52µs | O(N)* |
| BST (Sequential/Unbalanced) | ~88s | O(N)** | ~3.4ms | O(N)** |
| BST (Random/Balanced) | ~296ms | O(log N) | ~11µs | O(log N)*** |

*Page buffering dramatically reduces I/O operations, showing ~63x speedup over offset-based
**Sequential insertion creates completely unbalanced tree (degenerates to linked list)
***Random insertion creates balanced tree, showing ~300x speedup over unbalanced BST!

**Key Insights:**
1. **Linked Lists**: Fast insertion O(1) but slow search O(N)
2. **Page Buffering**: Same algorithmic complexity but 63x real-world speedup via I/O optimization
3. **Tree Balance is Critical**: Unbalanced BST degenerates to linked list performance (height = N)
4. **Balanced Tree Power**: Random insertion → balanced tree → true O(log N) performance (307x faster!)
5. **Why B-Trees**: Automatically maintain balance + page optimization = best of both worlds

## LinkedListStore Interface

All three storage implementations (Offset-Based, Page-Slotted, Binary Tree) satisfy a common interface:

```go
type LinkedListStore interface {
    Open(path string, truncate bool) (*Handle, error)
    AppendTail(h *Handle, value uint32) error
    DeleteFirstByValue(h *Handle, value uint32) (bool, error)
    TraverseValues(h *Handle) ([]uint32, error)
    Where(h *Handle, target uint32) (int64, error) // or *Location for paged
    Close(h *Handle) error
}
```

The paged version adds:
- `PrependHead(h *Handle, value uint32) error`
- `TraverseValuesPhysical(h *Handle) ([]uint32, error)`
- `Where` returns `*Location{Page, Slot}` instead of `int64` offset

## File Formats

**Magic Bytes:**
- Linked list files: `[4]byte{'L', 'L', 'S', 'T'}`
- Binary tree files: `[4]byte{'B', 'S', 'T', 'R'}`
- Comparison tool files: `[4]byte{'C', 'M', 'P', 'R'}`

**Offset-Based Linked List Format:**
- Header (32 bytes): Magic + Version + PageSize + HeadOffset + TailOffset + Size
- Nodes at arbitrary offsets: each 16 bytes

**Page-Slotted Linked List Format:**
- Header (32 bytes): Magic + Version + PageSize + PageCount + Head/Tail Page/Slot + Size
- Pages (4096 bytes each) starting after header
- Each page: PageHeader (2 bytes) + Slots (12 bytes each)

**Binary Search Tree Format:**
- Header (24 bytes): Magic + Version + PageSize + RootOffset + Size
- Nodes at arbitrary offsets: each 28 bytes

## Code Patterns and Conventions

**Binary Encoding:** All code uses `binary.BigEndian` for consistent byte ordering.

**Error Handling:** Functions return errors that should be checked. Example code typically uses `panic(err)` for simplicity (educational context).

**Logical Deletion:** All three implementations use tombstone flags rather than physical deletion to avoid compaction complexity.

**Handle Pattern:** Operations use a `Handle` struct containing the file and header, allowing multiple storage backends.

## Generated Files (Ignored by Git)

- `*.db`: Database files from chapter01 examples
- `*.llst`: Linked list storage files from chapter02
- `*.bst`: Binary search tree storage files from chapter03
- `test.txt`: Test data file
- `.gocache/`: Go build cache

## Important Implementation Details

**B-Tree Node Splitting (`main.go:48-82`):**
- Triggered when node has 2t-1 keys
- Median key promoted to parent
- Left/right children properly redistributed
- Handles both leaf and internal nodes

**Page Buffer Optimization (`chapter02/paged_linked_list/main.go:526-568`):**
- Single-page cache avoids repeated disk I/O
- Critical for linked list traversal where adjacent nodes often share pages
- `loadPage` only called when switching pages
- `readSlotWithBuffer` parses from cached byte array

**Slot Allocation Strategy (`chapter02/paged_linked_list/main.go:314-347`):**
- Attempts to use last page first (append-only optimization)
- Creates new page only when current page full
- Maintains sequential page allocation for better locality

**Binary Search Tree Insertion (`chapter03/binarytree/main.go:175-216`):**
- Recursive insertion following BST property
- Creates new node at file end when reaching null offset
- Updates parent's left/right pointer to link new node
- Demonstrates how sequential insertion creates unbalanced tree (worst case)

**Concurrency in Web Server:**
- Read operations (search, state) use `RLock()`
- Write operations (create, insert) use `Lock()`
- Single global tree protected by mutex (simple but sufficient for educational demo)
