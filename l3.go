package hookdb

import (
	"context"
	"fmt"
	"iter"
	"sync"
)

type l3Store struct {
	l2values    *l2valueStore
	l2hooks     *l2hookStore
	mu          *sync.RWMutex
	putCallback func(k, v []byte) error
}

func newL3Store() *l3Store {
	s := &l3Store{
		l2values: &l2valueStore{
			l1Store: newL1Store[[]byte](),
		},
		l2hooks: &l2hookStore{
			l1Store: newL1Store[HookHandler](),
		},
		mu: new(sync.RWMutex),
	}
	s.putCallback = func(k, v []byte) error {
		return hook(s.l2hooks, k, v)
	}
	return s
}

func (s *l3Store) Transaction() *l3TxnStore {
	return &l3TxnStore{
		l3Store: s.withL1Txn(),
		parent:  s.mu,
		closed:  false,
	}
}

func (s *l3Store) TransactionWithLock() *l3TxnStore {
	s.mu.Lock()
	return &l3TxnStore{
		l3Store: s.withL1Txn(),
		parent:  s.mu,
		inLock:  true,
		closed:  false,
	}
}

func (s *l3Store) withL1Txn() *l3Store {
	l3 := &l3Store{
		l2values: &l2valueStore{
			l1Store: newL1TxnStore(s.l2values.l1Store.(*l1BaseStore[[]byte])),
		},
		l2hooks: &l2hookStore{
			l1Store: newL1TxnStore(s.l2hooks.l1Store.(*l1BaseStore[HookHandler])),
		},
		putCallback: func(k, v []byte) error { return nil },
		mu:          new(sync.RWMutex),
	}
	return l3
}

func (s *l3Store) Put(k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.l2values.Exec(s.l2values.put, input[[]byte]{k: k, v: v})
	if err != nil {
		return err
	}
	return s.putCallback(k, v)
}

func (s *l3Store) Get(k []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, err := s.l2values.Exec(s.l2values.get, input[[]byte]{k: k})
	if err != nil {
		return nil, err
	}
	return o.val, nil
}

func (s *l3Store) Delete(k []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.l2values.Exec(s.l2values.delete, input[[]byte]{k: k})
	return err
}

func (s *l3Store) Query(ctx context.Context, k []byte, opts ...QueryOption) iter.Seq2[[]byte, error] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return func(yield func([]byte, error) bool) {
		for output, err := range s.l2values.Query(ctx, k, opts...) {
			if ok := yield(output.val, err); !ok {
				return
			}
		}
	}
}

func (s *l3Store) AppendHook(prefix []byte, fn HookHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.l2hooks.Exec(s.l2hooks.put, input[HookHandler]{k: prefix, v: fn})
	return err
}

func (s *l3Store) RemoveHook(prefix []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.l2hooks.Exec(s.l2hooks.delete, input[HookHandler]{k: prefix})
	return err
}

type l3TxnStore struct {
	*l3Store

	inLock bool
	parent *sync.RWMutex
	closed bool
}

func (s *l3TxnStore) Commit() error {
	if s.closed {
		return ErrClosedTransaction
	}
	if !s.inLock {
		s.parent.Lock()
	}
	defer func() {
		s.closed = true
		s.parent.Unlock()
	}()
	outputs, err := s.l2values.Commit()
	if err != nil {
		return err
	}
	for _, o := range outputs {
		if o.deleted {
			continue
		}
		err := hook(s.l2hooks, o.key, o.val)
		if err != nil {
			err = fmt.Errorf("%w: %w", err, s.l2values.Rollback())
			return err
		}
	}
	_, err = s.l2hooks.Commit()
	if err != nil {
		err = fmt.Errorf("%w: %w", err, s.l2values.Rollback())
		return err
	}
	return nil
}

func (s *l3TxnStore) Rollback() error {
	if s.closed {
		return ErrClosedTransaction
	}
	if !s.inLock {
		s.parent.Lock()
	}
	defer func() {
		s.closed = true
		s.parent.Unlock()
	}()
	return nil
}

func hook(l2 *l2hookStore, k, v []byte) error {
	for output, err := range l2.FoundPrefix(k) {
		if err != nil {
			return err
		}
		if output.deleted {
			continue
		}
		if output.val(k, v) {
			_, err := l2.Exec(l2.delete, input[HookHandler]{i: output.i})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
