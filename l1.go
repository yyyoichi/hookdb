package hookdb

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/google/btree"
)

type (
	l1BaseStore[T any] struct {
		nextI    int64
		iCounter iCounter
		mu       sync.RWMutex
		vals     map[int64]T
		keys     map[int64][]byte
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

func newL1Store[T any](opts ...storeOptionF) *l1BaseStore[T] {
	var op = &storeOptions{
		iCounter: upCounter,
		startI:   1,
	}
	for _, f := range opts {
		f(op)
	}

	var l1BaseStore = l1BaseStore[T]{
		nextI:    op.startI,
		iCounter: op.iCounter,
		vals:     map[int64]T{},
		keys:     map[int64][]byte{},
		btree: btree.NewG(2, func(a, b *item) bool {
			return bytes.Compare(a.k, b.k) == -1
		}),
	}
	return &l1BaseStore
}

func (s *l1BaseStore[T]) Btree() *btree.BTreeG[*item] {
	return s.btree
}

func (s *l1BaseStore[T]) Exec(cmd command[T], in input[T]) (output[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cmd(in)
}

func (s *l1BaseStore[T]) BatchExec(cmd command[T], inputs ...input[T]) ([]output[T], []error) {
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

func (s *l1BaseStore[T]) put(in input[T]) (o output[T], err error) {
	if len(in.k) == 0 {
		return o, ErrEmptyEntry
	}

	i := s.nextI
	s.keys[i] = in.k
	s.vals[i] = in.v
	_, _ = s.btree.ReplaceOrInsert(&item{k: in.k, i: i})
	s.iCounter(&s.nextI)

	o.key = in.k
	o.val = in.v
	o.i = i
	o.deleted = false
	return o, nil
}

func (s *l1BaseStore[T]) get(in input[T]) (o output[T], err error) {
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
	return
}

func (s *l1BaseStore[T]) delete(in input[T]) (o output[T], err error) {
	switch {
	case in.i != 0:
		k, found := s.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i
		_, _ = s.btree.Delete(&item{k: k})
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

func (s *l1BaseStore[T]) Commit() (os []output[T], err error) {
	return
}

func (s *l1BaseStore[T]) Rollback() error {
	return nil
}

type l1TxnStore[T any] struct {
	origin *l1BaseStore[T]
	*l1BaseStore[T]
	dels map[int64]bool
}

func newL1TxnStore[T any](origin *l1BaseStore[T]) *l1TxnStore[T] {
	s := &l1TxnStore[T]{
		origin:      origin,
		l1BaseStore: newL1Store[T](withDownCounter()),
		dels:        make(map[int64]bool),
	}
	s.l1BaseStore.btree = s.origin.btree.Clone()
	return s
}

func (s *l1TxnStore[T]) Btree() *btree.BTreeG[*item] {
	return s.l1BaseStore.btree
}

func (s *l1TxnStore[T]) put(in input[T]) (output[T], error) {
	o, _ := s.l1BaseStore.put(in)
	s.dels[o.i] = false
	return o, nil
}

func (s *l1TxnStore[T]) get(in input[T]) (o output[T], err error) {
	switch {
	case 0 < in.i:
		o, err = s.origin.get(in)
	case in.i < 0:
		o, err = s.l1BaseStore.get(in)
	default:
		// get with key, btree is cloned
		o, err = s.l1BaseStore.get(in)
		// found in origin l1BaseStore
		if 0 < o.i {
			o, err = s.origin.get(in)
		}
	}
	if err != nil {
		return o, err
	}
	o.deleted = s.dels[o.i]
	return o, nil
}

func (s *l1TxnStore[T]) delete(in input[T]) (o output[T], err error) {
	switch {
	case 0 < in.i:
		k, found := s.origin.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i

	case in.i < 0:
		k, found := s.l1BaseStore.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i

	case len(in.k) != 0:
		item, found := s.l1BaseStore.btree.Get(&item{k: in.k})
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

	switch {
	case 0 < o.i:
		o.val = s.origin.vals[o.i]
	case o.i < 0:
		o.val = s.l1BaseStore.vals[o.i]
	}
	// delete
	o, _ = s.l1BaseStore.put(input[T]{k: o.key, v: o.val})
	s.dels[o.i] = true
	o.deleted = true
	return o, err
}

// return inserted uniq outputs ordered by insert-time asc
func (s *l1TxnStore[T]) Commit() ([]output[T], error) {
	outputs := s.scan()
	_, err := s.merge(outputs)
	if err != nil {
		return nil, err
	}

	uniq := make(map[string][]byte, len(outputs))
	results := make([]output[T], 0, len(outputs))
	for _, o := range slices.Backward(outputs) {
		strKey := string(o.key)
		if v, found := uniq[strKey]; found && bytes.Equal(o.key, v) {
			continue
		}
		results = append(results, o)
		uniq[strKey] = o.key
	}
	slices.SortFunc(results, func(a, b output[T]) int {
		return int(b.i) - int(a.i)
	})
	return results, nil
}

func (s *l1TxnStore[T]) Rollback() error {
	outputs := s.scan()
	_, err := s.reverse(outputs)
	return err
}

func (s *l1TxnStore[T]) merge(outputs []output[T]) (int64, error) {
	s.l1BaseStore.mu.Lock()
	defer s.l1BaseStore.mu.Unlock()

	var success int64
	for _, o := range outputs {
		var err error
		switch o.deleted {
		case true:
			_, err = s.origin.delete(input[T]{k: o.key})
		case false:
			_, err = s.origin.put(input[T]{k: o.key, v: o.val})
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			err = fmt.Errorf("rollback: unexpected error: cannot merge [%v] : %w", o, err)
			if _, err := s.reverse(outputs[:success]); err != nil {
				panic(fmt.Errorf("cannot rollback: %w", err))
			}
			return 0, err
		}
		success++
	}
	return success, nil
}

func (s *l1TxnStore[T]) reverse(outputs []output[T]) (int64, error) {
	var success int64
	for _, o := range outputs {
		var err error
		switch o.deleted {
		case false:
			_, err = s.origin.delete(input[T]{k: o.key})
		case true:
			_, err = s.origin.put(input[T]{k: o.key, v: o.val})
		}
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			return success, err
		}
		success++
	}
	return success, nil
}

// scan by i desc
func (s *l1TxnStore[T]) scan() []output[T] {
	outputs := make([]output[T], 0, s.l1BaseStore.nextI*-1)
	var i int64 = -1
	for {
		if i == s.l1BaseStore.nextI {
			break
		}
		o := output[T]{
			key:     s.l1BaseStore.keys[i],
			val:     s.l1BaseStore.vals[i],
			i:       i,
			deleted: s.dels[i],
		}
		outputs = append(outputs, o)
		i--
	}
	return outputs
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
	upCounter       iCounter = func(i *int64) { *i += 1 }
	downCounter     iCounter = func(i *int64) { *i -= 1 }
	withDownCounter          = func() storeOptionF {
		return func(so *storeOptions) {
			so.startI = -1
			so.iCounter = downCounter
		}
	}
)
