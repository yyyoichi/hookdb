package hookdb

// in handler, cannot appned hook
type HookHandler func(innerId int64) (removeHook bool)

type db interface {
	Get(k []byte) ([]byte, error)
	Put(k []byte, v []byte) error
	Delete(k []byte) error
	AppendHook(prefix []byte, fn HookHandler) error
	RemoveHook(prefix []byte) error
}

type HookDB interface {
	db
	Transaction() Transaction
}

type Transaction interface {
	db
	Commit() error
	Rollback() error
}

func New() HookDB {
	return newL3Store()
}
