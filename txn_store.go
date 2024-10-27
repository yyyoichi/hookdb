package hookdb

import (
	"errors"
	"fmt"
)

type txnStore[T any] struct {
	origin *store[T]
	*store[T]
	dels map[int64]bool
}

func newTxnStore[T any](origin *store[T]) *txnStore[T] {
	s := &txnStore[T]{
		origin: origin,
		store:  newStore[T](withDownCounter()),
		dels:   make(map[int64]bool),
	}
	s.store.btree = s.origin.btree.Clone()
	return s
}

func (s *txnStore[T]) put(in input[T]) (output[T], error) {
	o, _ := s.store.put(in)
	s.dels[o.i] = false
	return o, nil
}

func (s *txnStore[T]) get(in input[T]) (o output[T], err error) {
	switch {
	case 0 < in.i:
		o, err = s.origin.get(in)
	case in.i < 0:
		o, err = s.store.get(in)
	default:
		// get with key, btree is cloned
		o, err = s.store.get(in)
		// found in origin store
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

func (s *txnStore[T]) delete(in input[T]) (o output[T], err error) {
	switch {
	case 0 < in.i:
		k, found := s.origin.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i

	case in.i < 0:
		k, found := s.store.keys[in.i]
		if !found {
			return o, ErrKeyNotFound
		}
		o.key = k
		o.i = in.i

	case len(in.k) != 0:
		item, found := s.store.btree.Get(&item{k: in.k})
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
		o.val = s.store.vals[o.i]
	}
	o.deleted = true
	// delete
	o, _ = s.store.put(input[T]{k: o.key, v: o.val})
	s.dels[o.i] = true
	return o, err
}

func (s *txnStore[T]) commit() error {
	outputs := s.scan()
	_, err := s.merge(outputs)
	return err
}

func (s *txnStore[T]) rollback() error {
	outputs := s.scan()
	_, err := s.reverse(outputs)
	return err
}

func (s *txnStore[T]) merge(outputs []output[T]) (int64, error) {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

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

func (s *txnStore[T]) reverse(outputs []output[T]) (int64, error) {
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

// scan by i asc
func (s *txnStore[T]) scan() []output[T] {
	outputs := make([]output[T], 0, s.store.nextI*-1)
	var i int64 = -1
	for {
		if i == s.store.nextI {
			break
		}
		o := output[T]{
			key:     s.store.keys[i],
			val:     s.store.vals[i],
			i:       i,
			deleted: s.dels[i],
		}
		outputs = append(outputs, o)
		i--
	}
	return outputs
}
