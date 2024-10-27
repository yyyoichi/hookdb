package hookdb

import (
	"bytes"
	"context"
	"sync"
)

// in handler, cannot appned hook
type HookHandler func(innerId int64) (removeHook bool)

type HookDB struct {
	v  *store[[]byte]
	h  *store[HookHandler]
	mu *sync.RWMutex
}

func New() *HookDB {
	var mu sync.RWMutex
	return &HookDB{
		v:  newStore[[]byte](),
		h:  newStore[HookHandler](),
		mu: &mu,
	}
}

func (b *HookDB) WithTransaction(ctx context.Context) *Txn {
	return newTxn(ctx, b)
}

func (b *HookDB) Commit(ctx context.Context) error {
	if txn, ok := (ctx).(*Txn); ok {
		return txn.Commit()
	}
	return nil
}

func (b *HookDB) Put(ctx context.Context, k, v []byte) error {
	in := input[[]byte]{k: k, v: v}
	if txn, ok := ctx.(*Txn); ok {
		_, err := txn.v.Exec(txn.v.put, in)
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.v.Exec(b.v.put, in)
	return err
}

func (b *HookDB) Get(ctx context.Context, k []byte) ([]byte, error) {
	in := input[[]byte]{k: k}
	if txn, ok := ctx.(*Txn); ok {
		o, err := txn.v.Exec(txn.v.put, in)
		return o.val, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	o, err := b.v.Exec(b.v.get, in)
	return o.val, err
}

func (b *HookDB) GetAt(ctx context.Context, innerId int64) ([]byte, error) {
	in := input[[]byte]{i: innerId}
	if txn, ok := ctx.(*Txn); ok {
		o, err := txn.v.Exec(txn.v.put, in)
		return o.val, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	o, err := b.v.Exec(b.v.get, in)
	return o.val, err
}

func (b *HookDB) Delete(ctx context.Context, k []byte) ([]byte, error) {
	in := input[[]byte]{k: k}
	if txn, ok := ctx.(*Txn); ok {
		o, err := txn.v.Exec(txn.v.put, in)
		return o.val, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	o, err := b.v.Exec(b.v.delete, in)
	return o.val, err
}

func (b *HookDB) AppendHook(ctx context.Context, prefix []byte, fn HookHandler) error {
	in := input[HookHandler]{k: prefix, v: fn}
	if txn, ok := ctx.(*Txn); ok {
		_, err := txn.h.Exec(txn.h.put, in)
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.h.Exec(b.h.put, in)
	return err
}

func (b *HookDB) RemoveHook(ctx context.Context, prefix []byte) error {
	in := input[HookHandler]{k: prefix}
	if txn, ok := ctx.(*Txn); ok {
		_, err := txn.h.Exec(txn.h.delete, in)
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.h.Exec(b.h.delete, in)
	return err
}

func (b *HookDB) callHook(innerId int64, k []byte) {
	b.h.mu.RLock()
	var deleteHookIds []int64
	b.h.btree.DescendLessOrEqual(&item{k: k}, func(item *item) bool {
		if item.k[0] != k[0] {
			return false
		}
		if !bytes.HasPrefix(k, item.k) {
			return true
		}
		fn, _ := b.h.getAt(item.i)
		if fn(innerId) {
			deleteHookIds = append(deleteHookIds, item.i)
		}
		return true
	})
	b.h.mu.RUnlock()
	b.h.bdeleteAt(deleteHookIds...)
}
