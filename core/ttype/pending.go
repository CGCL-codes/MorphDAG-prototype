package ttype

import "sync"

// ConcurPending records whether the pending tx is completed its execution
type ConcurPending struct {
	mu    sync.Mutex
	items []string
}

func (cp *ConcurPending) Append(item string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.items = append(cp.items, item)
}

func (cp *ConcurPending) BatchAppend(items []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.items = append(cp.items, items...)
}

func (cp *ConcurPending) Pop() string {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	last := cp.items[len(cp.items)-1]
	cp.items = cp.items[:len(cp.items)-1]
	return last
}

func (cp *ConcurPending) Delete(item string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	for i := range cp.items {
		if cp.items[i] == item {
			cp.items = append(cp.items[:i], cp.items[i+1:]...)
			break
		}
	}
}

func (cp *ConcurPending) Len() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.items)
}

func (cp *ConcurPending) Swap(i, j int) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.items[i], cp.items[j] = cp.items[j], cp.items[i]
}

func NewPending() *ConcurPending {
	return &ConcurPending{
		items: make([]string, 0, 50000),
	}
}
