package hookdb

import (
	"context"
	"iter"
)

// in handler, cannot appned hook
type HookHandler func(k, v []byte) (removeHook bool)

type HookDB struct {
	*DB
}

func New() *HookDB {
	return &HookDB{
		DB: &DB{
			l3: newL3Store(),
		},
	}
}

func (db *HookDB) Transaction() *Transaction {
	return &Transaction{
		DB: &DB{
			l3: db.l3.(*l3Store).Transaction(),
		},
	}
}

func (db *HookDB) TransactionWithLock() *Transaction {
	return &Transaction{
		DB: &DB{
			l3: db.l3.(*l3Store).TransactionWithLock(),
		},
	}
}

type Transaction struct {
	*DB
}

func (txn *Transaction) Commit() error {
	return txn.DB.l3.(*l3TxnStore).Commit()
}

func (txn *Transaction) Rollback() error {
	return txn.DB.l3.(*l3TxnStore).Rollback()
}

type (
	DB struct {
		l3 l3
	}
	l3 interface {
		Get(k []byte) ([]byte, error)
		Put(k []byte, v []byte) error
		Delete(k []byte) error
		Query(ctx context.Context, k []byte, opts ...QueryOption) iter.Seq2[[]byte, error]
		AppendHook(prefix []byte, fn HookHandler) error
		RemoveHook(prefix []byte) error
	}
)

func (db *DB) Get(k []byte) ([]byte, error) {
	return db.l3.Get(k)
}
func (db *DB) Put(k []byte, v []byte) error {
	return db.l3.Put(k, v)
}
func (db *DB) Delete(k []byte) error {
	return db.l3.Delete(k)
}
func (db *DB) Query(ctx context.Context, k []byte, opts ...QueryOption) iter.Seq2[[]byte, error] {
	return db.l3.Query(ctx, k, opts...)
}
func (db *DB) AppendHook(prefix []byte, fn HookHandler) error {
	return db.l3.AppendHook(prefix, fn)
}
func (db *DB) RemoveHook(prefix []byte) error {
	return db.l3.RemoveHook(prefix)
}
