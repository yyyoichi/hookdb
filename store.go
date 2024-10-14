package hookdb

import (
	"bytes"
	"sync"

	"github.com/google/btree"
)

type (
	store[T any] struct {
		nextI int64
		mu    sync.RWMutex
		vals  map[int64]T
		keys  map[int64][]byte
		btree *btree.BTreeG[*item]
	}
	item struct {
		k []byte
		i int64
	}
)

func newStore[T any]() store[T] {
	return store[T]{
		nextI: 1,
		vals:  map[int64]T{},
		keys:  map[int64][]byte{},
		btree: btree.NewG(2, func(a, b *item) bool {
			return bytes.Compare(a.k, b.k) == -1
		}),
	}
}

func (s *store[T]) put(k []byte, v T) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	i := s.nextI
	s.nextI++
	s.vals[i] = v
	s.keys[i] = k
	item := &item{k: k, i: i}
	s.btree.ReplaceOrInsert(item)

	return i
}

func (s *store[T]) get(k []byte) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var v T
	item, found := s.btree.Get(&item{k: k})
	if !found {
		return v, false
	}
	return s.vals[item.i], true
}

func (s *store[T]) getAt(i int64) (T, bool) {
	v, found := s.vals[i]
	return v, found
}

func (s *store[T]) delete(k []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, found := s.btree.Delete(&item{k: k})
	if !found {
		return
	}
	delete(s.vals, item.i)
	delete(s.keys, item.i)
}

func (s *store[T]) deleteAt(is ...int64) {
	if len(is) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, i := range is {
		k, found := s.keys[i]
		if !found {
			continue
		}
		delete(s.vals, i)
		delete(s.keys, i)
		_, _ = s.btree.Delete(&item{k: k})
	}
}
