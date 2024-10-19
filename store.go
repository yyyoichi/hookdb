package hookdb

import (
	"bytes"
	"iter"
	"sync"

	"github.com/google/btree"
)

type (
	store[T any] struct {
		nextI    int64
		iCounter iCounter
		mu       sync.RWMutex
		vals     map[int64]T
		keys     map[int64][]byte
		btree    *btree.BTreeG[*item]
	}
	item struct {
		// key
		k []byte
		// innerI
		i int64
		// delete flag
		d bool
	}
)

func newStore[T any](opts ...storeOptionF) store[T] {
	var op = &storeOptions{
		iCounter: upCounter,
		startI:   1,
	}
	for _, f := range opts {
		f(op)
	}
	return store[T]{
		nextI:    op.startI,
		iCounter: op.iCounter,
		vals:     map[int64]T{},
		keys:     map[int64][]byte{},
		btree: btree.NewG(2, func(a, b *item) bool {
			return bytes.Compare(a.k, b.k) == -1
		}),
	}
}

func (s *store[T]) put(e Entry[T]) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.p(e)
}

func (s *store[T]) bput(es ...Entry[T]) iter.Seq[int64] {
	if len(es) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var resp = make([]int64, len(es))
	for p, e := range es {
		resp[p] = s.p(e)
	}
	return func(yield func(int64) bool) {
		for _, r := range resp {
			ok := yield(r)
			if !ok {
				return
			}
		}
	}
}

func (s *store[T]) get(k []byte) (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.g(k)
}

func (s *store[T]) bget(ks ...[]byte) iter.Seq2[T, bool] {
	if len(ks) == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var vals, founds = make([]T, len(ks)), make([]bool, len(ks))
	for p, k := range ks {
		v, found := s.g(k)
		vals[p] = v
		founds[p] = found
	}

	return func(yield func(T, bool) bool) {
		for p := range len(ks) {
			ok := yield(vals[p], founds[p])
			if !ok {
				return
			}
		}
	}
}

func (s *store[T]) getAt(i int64) (T, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ga(i)
}

func (s *store[T]) bgetAt(is ...int64) iter.Seq2[T, bool] {
	if len(is) == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var vals, founds = make([]T, len(is)), make([]bool, len(is))
	for p, i := range is {
		v, found := s.ga(i)
		if !found {
			continue
		}
		vals[p] = v
		founds[p] = true
	}
	return func(yield func(T, bool) bool) {
		for p := range len(is) {
			ok := yield(vals[p], founds[p])
			if !ok {
				return
			}
		}
	}
}

func (s *store[T]) delete(k []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.d(k)
}

func (s *store[T]) bdelete(ks ...[]byte) {
	if len(ks) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, k := range ks {
		s.d(k)
	}
}

func (s *store[T]) deleteAt(i int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.da(i)
}

func (s *store[T]) bdeleteAt(is ...int64) {
	if len(is) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, i := range is {
		s.da(i)
	}
}

func (s *store[T]) p(e Entry[T]) int64 {
	i := s.nextI
	s.keys[i] = e.k
	s.vals[i] = e.v
	item := &item{k: e.k, i: i}
	s.btree.ReplaceOrInsert(item)
	s.iCounter(&s.nextI)
	return i
}

func (s *store[T]) g(k []byte) (v T, found bool) {
	item, found := s.btree.Get(&item{k: k})
	if !found {
		return
	}
	v = s.vals[item.i]
	found = true
	return
}

func (s *store[T]) d(k []byte) {
	item, found := s.btree.Delete(&item{k: k})
	if !found {
		return
	}
	delete(s.vals, item.i)
	delete(s.keys, item.i)
}

func (s *store[T]) ga(i int64) (v T, found bool) {
	v, found = s.vals[i]
	return
}

func (s *store[T]) da(i int64) {
	k, found := s.keys[i]
	if !found {
		return
	}
	delete(s.vals, i)
	delete(s.keys, i)
	_, _ = s.btree.Delete(&item{k: k})
}

type (
	iCounter     func(*int64)
	storeOptions struct {
		iCounter iCounter
		startI   int64
	}
	storeOptionF func(*storeOptions)
)

var (
	upCounter   iCounter = func(i *int64) { *i += 1 }
	downCounter iCounter = func(i *int64) { *i -= 1 }
)

var (
	withDownCounter = func() storeOptionF {
		return func(op *storeOptions) {
			op.startI = -1
			op.iCounter = downCounter
		}
	}
)
