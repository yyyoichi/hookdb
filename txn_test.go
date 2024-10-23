package hookdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransaction(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
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
}
