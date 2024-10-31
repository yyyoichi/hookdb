package hookdb

import (
	"bytes"
	"context"
	"iter"

	"github.com/google/btree"
)

type l1Store[T any] interface {
	BatchExec(cmd command[T], inputs ...input[T]) ([]output[T], []error)
	Btree() *btree.BTreeG[*item]
	Commit() (os []output[T], err error)
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

func (s *l2hookStore) FoundPrefix(k []byte) iter.Seq2[output[HookHandler], error] {
	return func(yield func(output[HookHandler], error) bool) {
		s.Btree().DescendLessOrEqual(&item{k: k}, func(item *item) bool {
			if item.k[0] != k[0] {
				return false
			}
			if !bytes.HasPrefix(k, item.k) {
				return true
			}
			output, err := s.get(input[HookHandler]{i: item.i})
			if ok := yield(output, err); !ok {
				return false
			}
			return true
		})
	}
}
