package hookdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		t.Parallel()
		db := New()
		// added
		output, err := db.v.put(input[[]byte]{k: []byte("key-1"), v: []byte("val-1")})
		assert.NoError(t, err)
		assert.Equal(t, "val-1", string(output.val))

		ctx := context.Background()
		txn := newTxn(ctx, db)
		output, err = txn.v.get(input[[]byte]{k: []byte("key-1")})
		assert.NoError(t, err)
		assert.Equal(t, "val-1", string(output.val))

		want := input[[]byte]{k: []byte("key-2"), v: []byte("val-2")}
		_, err = txn.v.put(want)
		assert.NoError(t, err)
		output, err = txn.v.get(want)
		assert.NoError(t, err)
		assert.Equal(t, "val-2", string(output.val))

		output, err = db.v.get(want)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		err = txn.Commit()
		assert.NoError(t, err)
		output, err = db.v.get(want)
		assert.NoError(t, err)
		assert.Equal(t, "val-2", string(output.val))

	})

	t.Run("double write", func(t *testing.T) {
		t.Parallel()
		db := New()

		ctx := context.Background()
		txn := newTxn(ctx, db)

		_, err := txn.v.put(input[[]byte]{k: []byte("key"), v: []byte("val")})
		assert.NoError(t, err)
		_, err = txn.v.put(input[[]byte]{k: []byte("key"), v: []byte("newval")})
		assert.NoError(t, err)

		err = txn.Commit()
		assert.NoError(t, err)

		output, err := db.v.get(input[[]byte]{k: []byte("key")})
		assert.NoError(t, err)
		assert.Equal(t, "newval", string(output.val))
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()
		db := New()

		want1 := input[[]byte]{k: []byte("key-1"), v: []byte("val-1")}
		want2 := input[[]byte]{k: []byte("key-2"), v: []byte("val-2")}

		_, err := db.v.put(want1)
		assert.NoError(t, err)
		_, err = db.v.put(want2)
		assert.NoError(t, err)

		ctx := context.Background()
		txn := newTxn(ctx, db)

		_, err = txn.v.delete(want1)
		assert.NoError(t, err)
		_, err = txn.v.delete(want2)
		assert.NoError(t, err)

		_, err = txn.v.put(input[[]byte]{k: want2.k, v: []byte("newval-2")})
		assert.NoError(t, err)

		want3 := input[[]byte]{k: []byte("key-3"), v: []byte("val-3")}
		_, err = txn.v.put(want3)
		assert.NoError(t, err)
		_, err = txn.v.delete(want3)
		assert.NoError(t, err)

		err = txn.Commit()
		assert.NoError(t, err)

		_, err = db.v.get(want1)
		assert.ErrorIs(t, err, ErrKeyNotFound)
		_, err = db.v.get(want3)
		assert.ErrorIs(t, err, ErrKeyNotFound)

		output, err := db.v.get(want2)
		assert.NoError(t, err)
		assert.Equal(t, "newval-2", string(output.val))
	})
}
