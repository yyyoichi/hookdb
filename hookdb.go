package hookdb

import (
	"context"
	"iter"
)

// in handler, cannot appned hook
type HookHandler func(k, v []byte) (removeHook bool)

type db interface {
	Get(k []byte) ([]byte, error)
	Put(k []byte, v []byte) error
	Delete(k []byte) error
	Query(ctx context.Context, k []byte) iter.Seq2[[]byte, error]
	AppendHook(prefix []byte, fn HookHandler) error
	RemoveHook(prefix []byte) error
}

type HookDB interface {
	db
	Transaction() Transaction
	TransactionWithLock() Transaction
}

type Transaction interface {
	db
	Commit() error
	Rollback() error
}

func New() HookDB {
	return newL3Store()
}
