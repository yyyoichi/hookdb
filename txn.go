package hookdb

import (
	"context"
	"fmt"
	"sync"
)

type Txn struct {
	context.Context
	v      *txnStore[[]byte]
	h      *txnStore[HookHandler]
	mu     *sync.RWMutex
	closed bool
}

func newTxn(ctx context.Context, db *HookDB) *Txn {
	txn := &Txn{
		Context: ctx,
		mu:      db.mu,
		v:       newTxnStore(db.v),
		h:       newTxnStore(db.h),
	}
	return txn
}

func (x *Txn) Commit() error {
	if x.closed {
		return ErrClosedTransaction
	}
	x.mu.Lock()
	defer func() {
		x.closed = true
		x.mu.Unlock()
	}()
	err := x.v.commit()
	if err != nil {
		return err
	}
	err = x.h.commit()
	if err != nil {
		err = fmt.Errorf("%w: %w", err, x.v.rollback())
		return err
	}
	return nil
}

func (x *Txn) Rollback() error {
	if x.closed {
		return ErrClosedTransaction
	}
	x.mu.Lock()
	defer func() {
		x.closed = true
		x.mu.Unlock()
	}()
	x.v.store.btree = x.v.origin.btree.Clone()
	x.v.dels = make(map[int64]bool)
	x.h.store.btree = x.h.origin.btree.Clone()
	x.h.dels = make(map[int64]bool)
	return nil
}
