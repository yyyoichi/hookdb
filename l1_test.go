package hookdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestL1Store(t *testing.T) {
	testSimpleOutput := func(t *testing.T, output output[string], expDel bool) {
		t.Helper()
		assert.Equal(t, "key", string(output.key))
		assert.Equal(t, "val", output.val)
		assert.NotZero(t, output.i)
		assert.Equal(t, expDel, output.deleted)
	}

	test := []struct {
		l1BaseStore *l1BaseStore[string]
	}{
		{newL1Store[string]()},
		{newL1Store[string](withDownCounter())},
	}
	for _, tt := range test {
		in := input[string]{
			k: []byte("key"),
			v: "val",
		}
		output, err := tt.l1BaseStore.put(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.l1BaseStore.get(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.l1BaseStore.delete(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.l1BaseStore.get(in)
		assert.Equal(t, ErrKeyNotFound, err)
		// reinput
		output, err = tt.l1BaseStore.put(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		// with i
		in = input[string]{i: output.i}
		output, err = tt.l1BaseStore.get(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.l1BaseStore.delete(in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.l1BaseStore.get(in)
		assert.Equal(t, ErrKeyNotFound, err)

		// with Exec
		in = input[string]{
			k: []byte("key"),
			v: "val",
		}
		output, err = tt.l1BaseStore.Exec(tt.l1BaseStore.put, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.l1BaseStore.Exec(tt.l1BaseStore.get, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, false)
		output, err = tt.l1BaseStore.Exec(tt.l1BaseStore.delete, in)
		assert.NoError(t, err)
		testSimpleOutput(t, output, true)
		_, err = tt.l1BaseStore.Exec(tt.l1BaseStore.get, in)
		assert.Equal(t, ErrKeyNotFound, err)

		// with batch
		outputs, errs := tt.l1BaseStore.BatchExec(tt.l1BaseStore.put, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], false)
		outputs, errs = tt.l1BaseStore.BatchExec(tt.l1BaseStore.get, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], false)
		outputs, errs = tt.l1BaseStore.BatchExec(tt.l1BaseStore.delete, in)
		assert.Len(t, errs, 0)
		assert.Len(t, outputs, 1)
		testSimpleOutput(t, outputs[0], true)
		outputs, errs = tt.l1BaseStore.BatchExec(tt.l1BaseStore.get, in)
		assert.Len(t, errs, 1)
		assert.Len(t, outputs, 0)
		assert.Equal(t, ErrKeyNotFound, errs[0])
	}
}

func TestL1TxnStore(t *testing.T) {
	origin := newL1Store[string]()
	txn := newL1TxnStore(origin)

	output, err := txn.put(input[string]{k: []byte("key"), v: "val"})
	assert.NoError(t, err)
	assert.Equal(t, "val", output.val)

	output, err = txn.get(input[string]{k: []byte("key")})
	assert.NoError(t, err)
	assert.Equal(t, "val", output.val)
	assert.False(t, output.deleted)

	output, err = txn.delete(input[string]{k: []byte("key")})
	assert.NoError(t, err)
	assert.Equal(t, "val", output.val)
	assert.True(t, output.deleted)

	output, err = txn.get(input[string]{k: []byte("key")})
	assert.NoError(t, err)
	assert.Equal(t, "val", output.val)
	assert.True(t, output.deleted)
}
