package hookdb

import (
	"fmt"
	"sync"
)

type l3Store struct {
	l2values *l2valueStore
	l2hooks  *l2hookStore
	mu       *sync.RWMutex

	// must set in txn
	parent *sync.RWMutex
	closed bool
}

func newL3Store() *l3Store {
	return &l3Store{
		l2values: &l2valueStore{
			l1Store: newL1Store[[]byte](),
		},
		l2hooks: &l2hookStore{
			l1Store: newL1Store[HookHandler](),
		},
		mu:     new(sync.RWMutex),
		closed: true,
	}
}

func (s *l3Store) Transaction() Transaction {
	return &l3Store{
		l2values: &l2valueStore{
			l1Store: newL1TxnStore(s.l2values.l1Store.(*l1BaseStore[[]byte])),
		},
		l2hooks: &l2hookStore{
			l1Store: newL1TxnStore(s.l2hooks.l1Store.(*l1BaseStore[HookHandler])),
		},
		mu:     new(sync.RWMutex),
		parent: s.mu,
		closed: false,
	}
}

func (s *l3Store) Put(k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.l2values.Exec(s.l2values.put, input[[]byte]{k: k, v: v})
	if err != nil {
		return err
	}
	for output, err := range s.l2hooks.FoundPrefix(k) {
		if err != nil {
			return err
		}
		if output.deleted {
			continue
		}
		if output.val(k, v) {
			_, err := s.l2hooks.Exec(s.l2hooks.delete, input[HookHandler]{i: output.i})
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (s *l3Store) Commit() error {
	if s.closed {
		return ErrClosedTransaction
	}
	s.parent.Lock()
	defer func() {
		s.closed = true
		s.parent.Unlock()
	}()
	err := s.l2values.Commit()
	if err != nil {
		return err
	}
	err = s.l2hooks.Commit()
	if err != nil {
		err = fmt.Errorf("%w: %w", err, s.l2values.Rollback())
		return err
	}
	return nil
}

func (s *l3Store) Rollback() error {
	if s.closed {
		return ErrClosedTransaction
	}
	s.parent.Lock()
	defer func() {
		s.closed = true
		s.parent.Unlock()
	}()
	return nil
}
