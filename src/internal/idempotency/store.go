package idempotency

import "sync"

type Store interface {
	Exists(key string) bool
	Mark(key string)
	UnMark(key string)
}

type MemoryStore struct {
	mu    sync.Mutex
	items map[string]bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{items: make(map[string]bool)}
}

func (s *MemoryStore) Exists(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[key]
}

func (s *MemoryStore) Mark(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[key] = true
}

func (s *MemoryStore) UnMark(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}
