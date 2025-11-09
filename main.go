package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type BTreeNode struct {
	keys     []int
	children []*BTreeNode
	isLeaf   bool
}

type BTree struct {
	root *BTreeNode
	t    int
}

func (x *BTreeNode) FindChildIndex(k int) int {
	lastIndex := len(x.keys)
	for i := 0; i < len(x.keys); i++ {
		if k < x.keys[i] {
			return i
		}
	}
	return lastIndex
}

func (x *BTreeNode) Search(k int) (*BTreeNode, int) {
	for i := 0; i < len(x.keys); i++ {
		if x.keys[i] == k {
			return x, i
		}
	}

	if x.isLeaf {
		return nil, -1
	}

	i := x.FindChildIndex(k)

	return x.children[i].Search(k)
}

func (x *BTreeNode) SplitChild(i int, t int) {
	y := x.children[i]
	median := t - 1
	z := &BTreeNode{
		keys:     make([]int, t-1),
		children: nil,
		isLeaf:   y.isLeaf,
	}

	midKey := y.keys[median]
	copy(z.keys, y.keys[median+1:])
	yTmp := make([]int, t-1)
	copy(yTmp, y.keys[:median])
	y.keys = yTmp

	if !y.isLeaf {
		z.children = make([]*BTreeNode, t)
		copy(z.children, y.children[median+1:])
		yChildren := make([]*BTreeNode, len(y.children[:median+1]))
		copy(yChildren, y.children[:median+1])
		y.children = yChildren
	}

	tmp := make([]int, len(x.keys)+1)
	tmp[i] = midKey
	copy(tmp[:i], x.keys[:i])
	copy(tmp[i+1:], x.keys[i:])
	x.keys = tmp

	childTmp := make([]*BTreeNode, len(x.children)+1)
	copy(childTmp[:i+1], x.children[:i+1])
	childTmp[i+1] = z
	copy(childTmp[i+2:], x.children[i+1:])
	x.children = childTmp
}

func (x *BTreeNode) InsertNonFull(k int, t int) {
	if x.isLeaf {
		tmp := make([]int, len(x.keys)+1)
		copy(tmp, x.keys)
		x.keys = tmp

		i := len(x.keys) - 2
		for ; i >= 0; i-- {
			if k < x.keys[i] {
				x.keys[i+1] = x.keys[i]
			} else {
				break
			}
		}
		x.keys[i+1] = k
	} else {
		idx := x.FindChildIndex(k)

		if len(x.children[idx].keys) == 2*t-1 {
			x.SplitChild(idx, t)

			if x.keys[idx] < k {
				idx++
			}
		}

		x.children[idx].InsertNonFull(k, t)
	}
}

func (b *BTree) Insert(k int) {
	if b.root == nil {
		b.root = &BTreeNode{
			keys:   []int{k},
			isLeaf: true,
		}
		return
	}

	if len(b.root.keys) == 2*b.t-1 {
		oldRoot := b.root
		node := &BTreeNode{
			isLeaf:   false,
			children: []*BTreeNode{oldRoot},
		}
		node.SplitChild(0, b.t)
		b.root = node
	}

	b.root.InsertNonFull(k, b.t)
}

func (b *BTree) SearchPath(k int) ([]string, bool) {
	if b.root == nil {
		return nil, false
	}
	trace := make([]string, 0)
	found := searchWithTrace(b.root, "root", k, &trace)
	return trace, found
}

func searchWithTrace(node *BTreeNode, label string, k int, trace *[]string) bool {
	*trace = append(*trace, label)

	i := 0
	for i < len(node.keys) && k > node.keys[i] {
		i++
	}
	if i < len(node.keys) && node.keys[i] == k {
		return true
	}

	if node.isLeaf || i >= len(node.children) {
		return false
	}

	childLabel := fmt.Sprintf("%s-%d", label, i)
	return searchWithTrace(node.children[i], childLabel, k, trace)
}

type VisualNode struct {
	Path     string        `json:"path"`
	Keys     []int         `json:"keys"`
	IsLeaf   bool          `json:"isLeaf"`
	Children []*VisualNode `json:"children"`
}

type statePayload struct {
	HasTree bool        `json:"hasTree"`
	T       int         `json:"t"`
	Tree    *VisualNode `json:"tree"`
}

var (
	treeMu      sync.RWMutex
	currentTree *BTree
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/state", handleState)
	mux.HandleFunc("/api/create", handleCreate)
	mux.HandleFunc("/api/insert", handleInsert)
	mux.HandleFunc("/api/search", handleSearch)

	addr := ":8080"
	log.Printf("B-Tree tutorial server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

func handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	respondJSON(w, http.StatusOK, snapshotState())
}

func handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}

	var payload struct {
		T int `json:"t"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "JSON 데이터를 해석할 수 없습니다.")
		return
	}

	if payload.T < 2 {
		writeError(w, http.StatusBadRequest, "차수 t 는 2 이상이어야 합니다.")
		return
	}

	treeMu.Lock()
	currentTree = &BTree{t: payload.T}
	state := snapshotStateLocked()
	treeMu.Unlock()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": "새로운 B-Tree 인스턴스를 만들었습니다.",
		"state":   state,
	})
}

func handleInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}

	var payload struct {
		Value int `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "JSON 데이터를 해석할 수 없습니다.")
		return
	}

	treeMu.Lock()
	defer treeMu.Unlock()

	if currentTree == nil {
		writeError(w, http.StatusBadRequest, "먼저 B-Tree 를 생성하세요.")
		return
	}

	currentTree.Insert(payload.Value)
	state := snapshotStateLocked()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("%d 값을 삽입했습니다.", payload.Value),
		"state":   state,
	})
}

func handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}

	var payload struct {
		Value int `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "JSON 데이터를 해석할 수 없습니다.")
		return
	}

	treeMu.RLock()
	defer treeMu.RUnlock()

	if currentTree == nil {
		writeError(w, http.StatusBadRequest, "먼저 B-Tree 를 생성하세요.")
		return
	}

	path, found := currentTree.SearchPath(payload.Value)
	state := snapshotStateLocked()

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("%d 값을 탐색했습니다.", payload.Value),
		"found":   found,
		"path":    path,
		"state":   state,
	})
}

func snapshotState() statePayload {
	treeMu.RLock()
	defer treeMu.RUnlock()
	return snapshotStateLocked()
}

func snapshotStateLocked() statePayload {
	if currentTree == nil {
		return statePayload{HasTree: false}
	}

	var tree *VisualNode
	if currentTree.root != nil {
		tree = buildVisualTree(currentTree.root)
	}

	return statePayload{
		HasTree: true,
		T:       currentTree.t,
		Tree:    tree,
	}
}

func buildVisualTree(root *BTreeNode) *VisualNode {
	if root == nil {
		return nil
	}
	return buildVisualNode(root, "root")
}

func buildVisualNode(node *BTreeNode, path string) *VisualNode {
	if node == nil {
		return nil
	}

	snapshot := &VisualNode{
		Path:   path,
		Keys:   append([]int(nil), node.keys...),
		IsLeaf: node.isLeaf,
	}

	if len(node.children) > 0 {
		snapshot.Children = make([]*VisualNode, len(node.children))
		for i, child := range node.children {
			snapshot.Children[i] = buildVisualNode(child, fmt.Sprintf("%s-%d", path, i))
		}
	}

	return snapshot
}

func methodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, "지원하지 않는 HTTP 메서드입니다.")
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if payload != nil {
		_ = json.NewEncoder(w).Encode(payload)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, map[string]string{"error": message})
}

const indexHTML = `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>B-Tree 시각화 튜토리얼</title>
<style>
:root {
    font-family: 'Segoe UI', system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    color: #111827;
    background: #f9fafb;
}
body {
    margin: 0;
    background: #eef2ff;
}
main {
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem 1.5rem 4rem;
}
h1 {
    font-size: 2rem;
    margin-bottom: 0.25rem;
}
p.lead {
    margin-top: 0;
    color: #4b5563;
}
.panel {
    background: #fff;
    border-radius: 16px;
    padding: 1.5rem;
    margin-top: 1.5rem;
    box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
}
.panel h2 {
    margin-top: 0;
}
form {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem;
    margin-bottom: 0.5rem;
}
input[type="number"] {
    flex: 1;
    min-width: 120px;
    padding: 0.65rem 0.75rem;
    border: 1px solid #c7d2fe;
    border-radius: 10px;
    font-size: 1rem;
}
button {
    border: none;
    border-radius: 10px;
    padding: 0.65rem 1.5rem;
    background: #4f46e5;
    color: #fff;
    font-weight: 600;
    cursor: pointer;
}
button:disabled,
input:disabled {
    opacity: 0.6;
    cursor: not-allowed;
}
.status {
    font-size: 0.95rem;
    color: #2563eb;
    min-height: 1.25rem;
}
.tree-container {
    margin-top: 1rem;
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
}
.node {
    display: inline-flex;
    flex-direction: column;
    align-items: center;
    border: 2px solid #4f46e5;
    border-radius: 12px;
    padding: 0.75rem;
    margin: 0.75rem;
    background: #fff;
    min-width: 80px;
    box-shadow: 0 6px 16px rgba(79, 70, 229, 0.1);
}
.node .keys {
    display: flex;
    gap: 0.4rem;
}
.node .keys span {
    background: #e0e7ff;
    border-radius: 8px;
    padding: 0.35rem 0.75rem;
    border: 1px solid #a5b4fc;
}
.children {
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
    margin-top: 0.5rem;
}
.placeholder {
    text-align: center;
    color: #6b7280;
}
.highlight {
    border-color: #f97316 !important;
    box-shadow: 0 0 0 3px rgba(249, 115, 22, 0.3);
}
ol#search-trace {
    padding-left: 1.25rem;
    color: #334155;
}
ol#search-trace li.result {
    margin-top: 0.5rem;
    font-weight: 600;
}
</style>
</head>
<body>
<main>
    <h1>B-Tree 삽입 & 탐색 시각화</h1>
    <p class="lead">차수(t)를 설정해 트리를 만든 뒤 값을 삽입하거나 탐색해 보세요. 서버에서 실제 B-Tree 메서드가 실행되고, 그 결과를 아래에서 시각화합니다.</p>

    <section class="panel">
        <h2>1. B-Tree 생성</h2>
        <form id="create-form">
            <input id="degree-input" type="number" min="2" placeholder="차수 t (2 이상)" required />
            <button type="submit">생성</button>
        </form>
        <p class="status" id="create-status"></p>
    </section>

    <section class="panel">
        <h2>2. 삽입 & 탐색</h2>
        <form id="insert-form">
            <input id="insert-input" type="number" placeholder="삽입할 값" required />
            <button type="submit">삽입</button>
        </form>
        <form id="search-form">
            <input id="search-input" type="number" placeholder="탐색할 값" required />
            <button type="submit">탐색</button>
        </form>
        <p class="status" id="action-status"></p>
    </section>

    <section class="panel">
        <h2>3. 현재 트리 상태</h2>
        <p id="tree-state">아직 트리가 없습니다. 먼저 차수를 입력해 생성하세요.</p>
        <div class="tree-container" id="tree-container">
            <div class="placeholder">시각화 할 노드가 없습니다.</div>
        </div>
    </section>

    <section class="panel">
        <h2>4. 탐색 경로</h2>
        <ol id="search-trace">
            <li>아직 탐색 기록이 없습니다.</li>
        </ol>
    </section>
</main>
<script>
const createForm = document.getElementById('create-form');
const insertForm = document.getElementById('insert-form');
const searchForm = document.getElementById('search-form');
const createStatus = document.getElementById('create-status');
const actionStatus = document.getElementById('action-status');
const treeContainer = document.getElementById('tree-container');
const treeState = document.getElementById('tree-state');
const traceList = document.getElementById('search-trace');
let currentTree = null;
let highlightedPaths = [];
toggleControls(false);

async function request(url, options = {}) {
    const response = await fetch(url, {
        headers: { 'Content-Type': 'application/json' },
        ...options,
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw data;
    }
    return data;
}

function applyState(state) {
    const hasTree = state.hasTree;
    currentTree = state.tree || null;
    treeState.textContent = hasTree
        ? '차수 t = ' + state.t + (currentTree ? ' / 노드 수: ' + countNodes(currentTree) : ' (아직 요소 없음)')
        : '아직 트리가 없습니다. 먼저 차수를 입력해 생성하세요.';
    renderTree(currentTree);
    toggleControls(hasTree);
}

function toggleControls(enabled) {
    ['insert-input', 'search-input'].forEach(id => {
        const el = document.getElementById(id);
        el.disabled = !enabled;
    });
    insertForm.querySelector('button').disabled = !enabled;
    searchForm.querySelector('button').disabled = !enabled;
}

function countNodes(node) {
    if (!node) return 0;
    let total = 1;
    if (node.children) {
        node.children.forEach(child => {
            total += countNodes(child);
        });
    }
    return total;
}

function renderTree(node) {
    treeContainer.innerHTML = '';
    if (!node) {
        treeContainer.innerHTML = '<div class="placeholder">시각화 할 노드가 없습니다.</div>';
        return;
    }
    treeContainer.appendChild(buildNodeElement(node));
}

function buildNodeElement(node) {
    const wrapper = document.createElement('div');
    wrapper.className = 'node';
    wrapper.dataset.path = node.path;

    const keysRow = document.createElement('div');
    keysRow.className = 'keys';
    node.keys.forEach(key => {
        const span = document.createElement('span');
        span.textContent = key;
        keysRow.appendChild(span);
    });
    if (!node.keys.length) {
        const span = document.createElement('span');
        span.textContent = '∅';
        keysRow.appendChild(span);
    }
    wrapper.appendChild(keysRow);

    if (node.children && node.children.length) {
        const childrenRow = document.createElement('div');
        childrenRow.className = 'children';
        node.children.forEach(child => {
            childrenRow.appendChild(buildNodeElement(child));
        });
        wrapper.appendChild(childrenRow);
    }

    return wrapper;
}

function highlightPath(paths) {
    highlightedPaths.forEach(path => {
        const el = treeContainer.querySelector('[data-path="' + path + '"]');
        if (el) {
            el.classList.remove('highlight');
        }
    });
    highlightedPaths = paths || [];
    highlightedPaths.forEach(path => {
        const el = treeContainer.querySelector('[data-path="' + path + '"]');
        if (el) {
            el.classList.add('highlight');
        }
    });
}

function renderTrace(paths, found) {
    traceList.innerHTML = '';
    if (!paths || !paths.length) {
        traceList.innerHTML = '<li>아직 탐색 기록이 없습니다.</li>';
        return;
    }

    paths.forEach((path, idx) => {
        const li = document.createElement('li');
        const node = getNodeByPath(path);
        const keys = node ? node.keys.join(', ') : '노드 정보 없음';
        li.textContent = '단계 ' + (idx + 1) + ': [' + keys + ']';
        traceList.appendChild(li);
    });

    const result = document.createElement('li');
    result.className = 'result';
    result.textContent = found ? '✅ 값을 찾았습니다!' : '❌ 해당 값은 트리에 없습니다.';
    traceList.appendChild(result);
}

function getNodeByPath(path) {
    if (!currentTree || !path) return null;
    const segments = path.split('-').slice(1);
    let node = currentTree;
    for (const segment of segments) {
        const idx = Number(segment);
        if (!node.children || !node.children[idx]) {
            return null;
        }
        node = node.children[idx];
    }
    return node;
}

createForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const t = Number(document.getElementById('degree-input').value);
    try {
        const data = await request('/api/create', {
            method: 'POST',
            body: JSON.stringify({ t })
        });
        createStatus.textContent = data.message;
        applyState(data.state);
        highlightPath([]);
        renderTrace([], false);
    } catch (err) {
        createStatus.textContent = err.error || '생성에 실패했습니다.';
    }
});

insertForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const value = Number(document.getElementById('insert-input').value);
    try {
        const data = await request('/api/insert', {
            method: 'POST',
            body: JSON.stringify({ value })
        });
        actionStatus.textContent = data.message;
        applyState(data.state);
        document.getElementById('insert-input').value = '';
        highlightPath([]);
        renderTrace([], false);
    } catch (err) {
        actionStatus.textContent = err.error || '삽입에 실패했습니다.';
    }
});

searchForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const value = Number(document.getElementById('search-input').value);
    try {
        const data = await request('/api/search', {
            method: 'POST',
            body: JSON.stringify({ value })
        });
        actionStatus.textContent = data.message;
        applyState(data.state);
        highlightPath(data.path || []);
        renderTrace(data.path, data.found);
    } catch (err) {
        actionStatus.textContent = err.error || '탐색에 실패했습니다.';
    }
});

(async function init() {
    try {
        const state = await fetch('/api/state').then(res => res.json());
        applyState(state);
    } catch (err) {
        console.error('초기 상태 로드 실패', err);
    }
})();
</script>
</body>
</html>`
