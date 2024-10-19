package hookdb

import (
	"bytes"
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
		dels     map[int64]bool
		btree    *btree.BTreeG[*item]
	}
	// bree item
	item struct {
		// key
		k []byte
		// innerI
		i int64
	}

	input[T any] struct {
		k []byte
		v T
		i int64
	}

	output[T any] struct {
		key     []byte
		val     T
		i       int64
		deleted bool
	}

	command[T any] func(input[T]) (output[T], error)
)

func newStore[T any](opts ...storeOptionF) *store[T] {
	var op = &storeOptions{
		iCounter: upCounter,
		startI:   1,
	}
	for _, f := range opts {
		f(op)
	}

	var store = store[T]{
		nextI:    op.startI,
		iCounter: op.iCounter,
		vals:     map[int64]T{},
		keys:     map[int64][]byte{},
		btree: btree.NewG(2, func(a, b *item) bool {
			return bytes.Compare(a.k, b.k) == -1
		}),
	}

	if op.theoryDeleted {
		store.dels = map[int64]bool{}
	}

	return &store
}

func (s *store[T]) Exec(cmd command[T], in input[T]) (output[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cmd(in)
}

func (s *store[T]) BatchExec(cmd command[T], inputs ...input[T]) ([]output[T], []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	outs, errs := make([]output[T], 0, len(inputs)), make([]error, 0, len(inputs))
	for _, in := range inputs {
		output, err := cmd(in)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		outs = append(outs, output)
	}
	return outs, errs
}

func (s *store[T]) put(in input[T]) (o output[T], err error) {
	if len(in.k) == 0 {
		return o, ErrEmptyEntry
	}

	i := s.nextI
	s.keys[i] = in.k
	s.vals[i] = in.v
	if s.dels != nil {
		s.dels[i] = false
	}
	_, _ = s.btree.ReplaceOrInsert(&item{k: in.k, i: i})
	s.iCounter(&s.nextI)

	o.key = in.k
	o.val = in.v
	o.i = i
	o.deleted = false
	return o, nil
}

func (s *store[T]) get(in input[T]) (o output[T], err error) {
	switch {
	case in.i != 0:
		k, found := s.keys[in.i]
		if !found {
			err = ErrKeyNotFound
			break
		}
		o.key = k
		o.i = in.i
	case len(in.k) != 0:
		item, found := s.btree.Get(&item{k: in.k})
		if !found {
			err = ErrKeyNotFound
			break
		}
		o.key = in.k
		o.i = item.i
	default:
		err = ErrEmptyEntry
	}
	if err != nil {
		return
	}
	o.val = s.vals[o.i]
	if s.dels != nil {
		o.deleted = s.dels[o.i]
	}
	if o.deleted {
		return o, ErrDeleted
	}
	return
}

func (s *store[T]) delete(in input[T]) (o output[T], err error) {
	if s.dels == nil {
		return s.physicalDelete(in)
	}
	return s.theoryDeleted(in)
}

func (s *store[T]) physicalDelete(in input[T]) (o output[T], err error) {
	switch {
	case in.i != 0:
		k, found := s.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i
	case len(in.k) != 0:
		item, found := s.btree.Delete(&item{k: in.k})
		if !found {
			err = ErrKeyNotFound
		}
		o.key = in.k
		o.i = item.i
	default:
		err = ErrEmptyEntry
	}
	if err != nil {
		return o, err
	}
	o.deleted = true
	o.val = s.vals[o.i]
	delete(s.vals, o.i)
	delete(s.keys, o.i)
	return
}

func (s *store[T]) theoryDeleted(in input[T]) (o output[T], err error) {
	switch {
	case in.i != 0:
		k, found := s.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i
	case len(in.k) != 0:
		item, found := s.btree.Get(&item{k: in.k})
		if !found {
			err = ErrKeyNotFound
		}
		o.key = in.k
		o.i = item.i
	default:
		err = ErrEmptyEntry
	}
	if err != nil {
		return o, err
	}
	o.val = s.vals[o.i]
	o.deleted = true
	// delete
	s.dels[o.i] = true
	return
}

type (
	iCounter     func(*int64)
	storeOptions struct {
		iCounter      iCounter
		startI        int64
		theoryDeleted bool
	}
	storeOptionF func(*storeOptions)
)

var (
	upCounter   iCounter = func(i *int64) { *i += 1 }
	downCounter iCounter = func(i *int64) { *i -= 1 }
)

var (
	withDownCounter = func() storeOptionF {
		return func(so *storeOptions) {
			so.startI = -1
			so.iCounter = downCounter
		}
	}
	withTheoryDeleted = func() storeOptionF {
		return func(so *storeOptions) {
			so.theoryDeleted = true
		}
	}
)
