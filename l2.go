package hookdb

import (
	"context"
	"iter"

	"github.com/google/btree"
)

type l1Store[T any] interface {
	BatchExec(cmd command[T], inputs ...input[T]) ([]output[T], []error)
	Btree() *btree.BTreeG[*item]
	Commit() error
	Exec(cmd command[T], in input[T]) (output[T], error)
	Rollback() error
	delete(in input[T]) (o output[T], err error)
	get(in input[T]) (o output[T], err error)
	put(in input[T]) (o output[T], err error)
}

type l2valueStore struct {
	l1Store[[]byte]
}

func (s *l2valueStore) Query(ctx context.Context, k []byte) iter.Seq[output[[]byte]] {
	return nil
}

type l2hookStore struct {
	l1Store[HookHandler]
}

func (s *l2hookStore) FoundPrefix(k []byte) iter.Seq[output[HookHandler]] {
	return nil
}
