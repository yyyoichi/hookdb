package hookdb_test

import (
	"fmt"
	"log"
	"sync"

	"github.com/yyyoichi/hookdb"
)

func Example() {
	db := hookdb.New()
	hooks := map[string]hookdb.HookHandler{
		"USER": func(innerId int64) (removeHook bool) {
			_, found := db.GetAt(innerId)
			if !found {
				log.Fatal("not found")
			}
			fmt.Println("put user!")
			return true // remove hook
		},
		"GAME": func(innerId int64) (removeHook bool) {
			_, found := db.GetAt(innerId)
			if !found {
				log.Fatal("not found")
			}
			fmt.Println("put game!")
			return false // remove no hook
		},
	}
	for prefix, fn := range hooks {
		db.AppendHook([]byte(prefix), fn)
	}

	db.Put([]byte("USER#101"), []byte("Taro"))
	db.Put([]byte("USER#102"), []byte("Hanako"))
	db.Put([]byte("GAME#201"), []byte("minesuper"))
	db.Put([]byte("GAME#202"), []byte("poker"))
	db.Put([]byte("NONE"), []byte("NONE"))

	// Output:
	// put user!
	// put game!
	// put game!
}

func Example_goroutine() {
	// HookDB is thread safe.
	db := hookdb.New()
	{
		var wg sync.WaitGroup
		for i := range 10000 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
			}()
		}
		wg.Wait()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, found := db.Get([]byte("key101"))
		if !found {
			log.Fatal("exp got")
		}
		fmt.Println(string(v))
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, found := db.Get([]byte("key9991"))
		if !found {
			log.Fatal("exp got")
		}
		fmt.Println(string(v))
	}()
	wg.Wait()

	// Unordered output:
	// val101
	// val9991
}

func ExampleHookDB_Put() {
	db := hookdb.New()
	db.Put([]byte("key"), []byte("apple"))
	db.Put([]byte("key"), []byte("orange"))
	db.Put([]byte("key"), []byte("cherry"))
	v, found := db.Get([]byte("key"))
	if !found {
		log.Fatal("exp got")
	}
	fmt.Println(string(v))

	// Output:
	// cherry
}

func ExampleHookDB_Get() {
	db := hookdb.New()
	db.Put([]byte("apple"), []byte("10"))
	db.Put([]byte("orange"), []byte("5"))
	v, found := db.Get([]byte("apple"))
	if !found {
		log.Fatal("exp got")
	}
	fmt.Println(string(v))
	_, found = db.Get([]byte("cherry"))
	fmt.Println(found)

	// Output:
	// 10
	// false
}

func ExampleHookDB_GetAt() {
	db := hookdb.New()
	db.Put([]byte("GAME#1001#MONSTER#01"), []byte("Lv.20"))
	db.AppendHook([]byte("GAME#1001"), func(innerId int64) (removeHook bool) {
		// exp) use in HookHandler
		v, _ := db.GetAt(innerId)
		fmt.Printf("got '%s'\n", string(v))
		return string(v) == "Lv.100"
	})
	db.Put([]byte("GAME#1001#MONSTER#02"), []byte("Lv.50"))
	db.Put([]byte("GAME#1002#MONSTER#01"), []byte("Lv.100")) // GAME#1002
	db.Put([]byte("GAME#1001#MONSTER#03"), []byte("Lv.12"))
	db.Put([]byte("GAME#1001#MONSTER#04"), []byte("Lv.100")) // !
	db.Put([]byte("GAME#1001#MONSTER#05"), []byte("Lv.72"))

	// Output:
	// got 'Lv.50'
	// got 'Lv.12'
	// got 'Lv.100'
}

func ExampleHookDB_Delete() {
	db := hookdb.New()
	db.Put([]byte("cherry"), []byte("10"))
	db.Delete([]byte("cherry"))

	_, found := db.Get([]byte("cherry"))
	fmt.Println(found)
	// Output:
	// false
}

func ExampleHookDB_AppendHook() {
	db := hookdb.New()
	db.AppendHook([]byte("c"), func(innerId int64) (removeHook bool) {
		fmt.Println("found 'c'")
		return false
	})
	db.Put([]byte("cherry"), []byte("100g"))
	db.Put([]byte("Chicago"), []byte("100km"))
	db.Put([]byte("curry"), []byte("1000yen"))
	db.AppendHook([]byte("c"), func(innerId int64) (removeHook bool) {
		fmt.Println("Ahaha!")
		return false
	})
	db.Put([]byte("chicken"), []byte("1kg"))

	// Output:
	// found 'c'
	// found 'c'
	// Ahaha!
}

func ExampleHookDB_RemoveHook() {
	db := hookdb.New()
	db.AppendHook([]byte("c"), func(innerId int64) (removeHook bool) {
		fmt.Println("found 'c'")
		return false
	})
	db.Put([]byte("cherry"), []byte("100g"))
	db.Put([]byte("Chicago"), []byte("100km"))
	db.RemoveHook([]byte("c"))
	db.Put([]byte("curry"), []byte("1000yen"))
	// Output:
	// found 'c'
}
