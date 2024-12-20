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

func (s *l2valueStore) Query(ctx context.Context, k []byte, opts ...QueryOption) iter.Seq2[output[[]byte], error] {
	var qo QueryOptions
	for _, opt := range opts {
		_ = opt(&qo)
	}
	var (
		iterate func(btree.ItemIteratorG[*item])
		skip    func(*item) bool
	)
	if qo.Reverse {
		// increment last byte
		l := len(k)
		kk := make([]byte, l)
		_ = copy(kk, k)
		kk[l-1] += 1
		iterate = func(iig btree.ItemIteratorG[*item]) {
			s.Btree().DescendLessOrEqual(&item{k: kk}, iig)
		}
		skip = func(item *item) bool {
			return bytes.Compare(k, item.k) == -1 && !bytes.HasPrefix(item.k, k)
		}
	} else {
		iterate = func(iig btree.ItemIteratorG[*item]) {
			s.Btree().AscendGreaterOrEqual(&item{k: k}, iig)
		}
		skip = func(*item) bool { return false }
	}
	return func(yield func(output[[]byte], error) bool) {
		iterate(func(item *item) bool {
			if skip(item) {
				return true
			}
			if !bytes.HasPrefix(item.k, k) {
				return false
			}
			output, err := s.get(input[[]byte]{i: item.i})
			if ok := yield(output, err); !ok {
				return false
			}
			return true
		})
	}
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
