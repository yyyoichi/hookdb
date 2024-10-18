package hookdb

type Entry[T any] struct {
	k []byte
	v T
}

func NewEntry[T any](k []byte, v T) Entry[T] {
	return Entry[T]{k: k, v: v}
}
