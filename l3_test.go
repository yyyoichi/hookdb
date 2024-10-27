package hookdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		t.Parallel()
		db := New()
		// added
		err := db.Put([]byte("key-1"), []byte("val-1"))
		assert.NoError(t, err)

		txn := db.Transaction()
		val, err := txn.Get([]byte("key-1"))
		assert.NoError(t, err)
		assert.Equal(t, "val-1", string(val))

		err = txn.Put([]byte("key-2"), []byte("val-2"))
		assert.NoError(t, err)
		val, err = txn.Get([]byte("key-2"))
		assert.NoError(t, err)
		assert.Equal(t, "val-2", string(val))

		_, err = db.Get([]byte("key-2"))
		assert.ErrorIs(t, err, ErrKeyNotFound)

		err = txn.Commit()
		assert.NoError(t, err)
		val, err = db.Get([]byte("key-2"))
		assert.NoError(t, err)
		assert.Equal(t, "val-2", string(val))

	})

	t.Run("double write", func(t *testing.T) {
		t.Parallel()
		db := New()

		txn := db.Transaction()

		err := txn.Put([]byte("key"), []byte("val"))
		assert.NoError(t, err)
		err = txn.Put([]byte("key"), []byte("newval"))
		assert.NoError(t, err)

		err = txn.Commit()
		assert.NoError(t, err)

		val, err := db.Get([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, "newval", string(val))
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		db := New()

		key1, key2, key3 := []byte("key-1"), []byte("key-2"), []byte("key-3")
		want1, want2, want3 := []byte("val-1"), []byte("val-2"), []byte("val-3")

		err := db.Put(key1, want1)
		assert.NoError(t, err)
		err = db.Put(key2, want2)
		assert.NoError(t, err)

		txn := db.Transaction()

		err = txn.Delete(key1)
		assert.NoError(t, err)
		err = txn.Delete(key2)
		assert.NoError(t, err)

		err = txn.Put(key2, []byte("newval-2"))
		assert.NoError(t, err)

		err = txn.Put(key3, want3)
		assert.NoError(t, err)
		err = txn.Delete(key3)
		assert.NoError(t, err)

		err = txn.Commit()
		assert.NoError(t, err)

		_, err = db.Get(want1)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		_, err = db.Get(want3)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		val, err := db.Get(key2)
		assert.NoError(t, err)
		assert.Equal(t, "newval-2", string(val))
	})
}
