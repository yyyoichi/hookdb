package hookdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T) {
	var C = 10_000
	store := newStore[string]()
	// add 10_000
	{
		var wg sync.WaitGroup
		for i := range C {
			wg.Add(1)
			go func() {
				defer wg.Done()
				store.put([]byte(fmt.Sprintf("key%d", i)), fmt.Sprintf("val%d", i))
			}()
		}
		wg.Wait()
	}
	// get
	{
		var wg sync.WaitGroup
		for i := range C {
			wg.Add(1)
			go func() {
				defer wg.Done()
				k := fmt.Sprintf("key%d", i)
				v, found := store.get([]byte(k))
				assert.True(t, found)
				assert.Equal(t, fmt.Sprintf("val%d", i), v)
			}()
		}
		wg.Wait()
	}
	// delete
	{
		var wg sync.WaitGroup
		for i := 0; i < C; i += 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				k := fmt.Sprintf("key%d", i)
				store.delete([]byte(k))
			}()
		}
		wg.Wait()
	}
	// getAt and deleteAt
	{
		innerIdCh := make(chan int64)
		go func() {
			defer close(innerIdCh)
			var wg sync.WaitGroup
			for i := range store.nextI {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, found := store.getAt(i)
					if found {
						innerIdCh <- i
					}
				}()
			}
			wg.Wait()
		}()
		ids := make([]int64, 0, int(store.nextI))
		for id := range innerIdCh {
			ids = append(ids, id)
		}
		store.deleteAt(ids...)
	}
	// get
	{
		var wg sync.WaitGroup
		for i := range C {
			wg.Add(1)
			go func() {
				defer wg.Done()
				k := fmt.Sprintf("key%d", i)
				_, found := store.get([]byte(k))
				assert.False(t, found)
			}()
		}
		wg.Wait()
	}
}
