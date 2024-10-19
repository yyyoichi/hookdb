package hookdb

import "errors"

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrEmptyEntry        = errors.New("entry(i,k) cannot be empty")
	ErrDeleted           = errors.New("deleted")
	ErrClosedTransaction = errors.New("transaction is closed")
)
