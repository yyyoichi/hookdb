package hookdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	var C = 10_000
	type V = string
	store := newStore[V]()
	// add 10_000
	{
		var req = make([]Entry[V], C)
		for i := range C {
			req[i] = NewEntry([]byte(fmt.Sprintf("key%d", i)), fmt.Sprintf("val%d", i))
		}
		store.bput(req...)
	}
	// get
	{
		var req = make([][]byte, C)
		for i := range C {
			req[i] = []byte(fmt.Sprintf("key%d", i))
		}
		var i int
		for v, found := range store.bget(req...) {
			assert.True(t, found)
			assert.Equal(t, fmt.Sprintf("val%d", i), v)
			i++
		}
	}
	// not found
	{
		_, found := store.get([]byte("not found"))
		assert.False(t, found)
	}
	// delete
	{
		var req = make([][]byte, 0, C/2)
		for i := 0; i < C; i += 2 {
			req = append(req, []byte(fmt.Sprintf("key%d", i)))
		}
		store.bdelete(req...)
	}
	// getAt and deleteAt
	{
		var req = make([]int64, 0, C)
		for i := range store.nextI {
			_, found := store.getAt(i)
			if found {
				req = append(req, i)
			}
		}
		store.bdeleteAt(req...)
	}
	// get
	{
		var req = make([]int64, C)
		for i := range C {
			req[i] = int64(i)
		}
		for _, found := range store.bgetAt(req...) {
			assert.False(t, found)
		}
	}
}
