package hookdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	testSimpleOutput := func(t *testing.T, output output[string], expDel bool) {
		t.Helper()
		assert.Equal(t, "key", string(output.key))
		assert.Equal(t, "val", output.val)
		assert.NotZero(t, output.i)
		assert.Equal(t, expDel, output.deleted)
	}

	test := []struct {
		store                  *store[string]
		expDeletedItemGetError error
	}{
		{newStore[string](), ErrKeyNotFound},
		{newStore[string](withDownCounter()), ErrKeyNotFound},
		{newStore[string](withDownCounter(), withTheoryDeleted()), ErrDeleted},
		{newStore[string](withTheoryDeleted()), ErrDeleted},
	}
	for _, tt := range test {
		in := input[string]{
			k: []byte("key"),
			v: "val",
		}
		output, err := tt.store.put(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.store.get(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.store.delete(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.store.get(in)
		assert.Equal(t, tt.expDeletedItemGetError, err)
		// reinput
		output, err = tt.store.put(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		// with i
		in = input[string]{i: output.i}
		output, err = tt.store.get(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.store.delete(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.store.get(in)
		assert.Equal(t, tt.expDeletedItemGetError, err)

		// with Exec
		in = input[string]{
			k: []byte("key"),
			v: "val",
		}
		output, err = tt.store.Exec(tt.store.put, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.store.Exec(tt.store.get, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.store.Exec(tt.store.delete, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.store.Exec(tt.store.get, in)
		assert.Equal(t, tt.expDeletedItemGetError, err)

		// with batch
		outputs, errs := tt.store.BatchExec(tt.store.put, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], false)
		outputs, errs = tt.store.BatchExec(tt.store.get, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], false)
		outputs, errs = tt.store.BatchExec(tt.store.delete, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], true)
		outputs, errs = tt.store.BatchExec(tt.store.get, in)
		assert.Len(t, errs, 1)
		assert.Len(t, outputs, 0)
		assert.Equal(t, tt.expDeletedItemGetError, errs[0])
	}
}
