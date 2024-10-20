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

	})
}
