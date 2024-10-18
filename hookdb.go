package hookdb

import "bytes"

// in handler, cannot appned hook
type HookHandler func(innerId int64) (removeHook bool)

type HookDB struct {
	v store[[]byte]
	h store[HookHandler]
}

func New() HookDB {
	return HookDB{
		v: newStore[[]byte](),
		h: newStore[HookHandler](),
	}
}

func (b *HookDB) Put(k, v []byte) {
	innerId := b.v.put(NewEntry(k, v))
	b.callHook(innerId, k)
}

func (b *HookDB) Get(k []byte) ([]byte, bool) {
	return b.v.get(k)
}

func (b *HookDB) GetAt(innerId int64) ([]byte, bool) {
	return b.v.getAt(innerId)
}

func (b *HookDB) Delete(k []byte) {
	b.v.delete(k)
}

func (b *HookDB) AppendHook(prefix []byte, fn HookHandler) {
	b.h.put(NewEntry(prefix, fn))
}

func (b *HookDB) RemoveHook(prefix []byte) {
	b.h.delete(prefix)
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
