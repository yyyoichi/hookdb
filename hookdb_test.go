package hookdb_test

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/yyyoichi/hookdb"
)

func Example() {
	db := hookdb.New()
	err := db.AppendHook([]byte("c"), func(k, v []byte) (removeHook bool) {
		fmt.Printf("%s: %s\n", k, v)
		return false
	})
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put([]byte("apple"), []byte("10kg"))
	if err != nil {
		log.Fatal(err)
	}

	// exp hit
	err = db.Put([]byte("chocolate"), []byte("100g"))
	if err != nil {
		log.Fatal(err)
	}

	err = db.Put([]byte("car"), []byte("3t"))
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// chocolate: 100g
	// car: 3t
}

func ExampleDB_Put() {
	db := hookdb.New()
	err := db.Put([]byte("apple"), []byte("10kg"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("apple"), []byte("9kg"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("apple"), []byte("8kg"))
	if err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("apple"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(v))

	// Output:
	// 8kg
}

func ExampleDB_Get() {
	db := hookdb.New()
	err := db.Put([]byte("town"), []byte("Yokohama"))
	if err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("town"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(v))

	v, err = db.Get([]byte("town"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(v))

	// Output:
	// Yokohama
	// Yokohama
}

func ExampleDB_Delete() {
	db := hookdb.New()
	err := db.Put([]byte("town"), []byte("Yokohama"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("town"), []byte("Sapporo"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Delete([]byte("town"))
	if err != nil {
		log.Fatal(err)
	}

	v, err := db.Get([]byte("town"))
	if !errors.Is(err, hookdb.ErrKeyNotFound) {
		log.Fatal(err)
	}
	fmt.Println(string(v))

	// Output:
	//
}

func ExampleDB_AppendHook() {
	db := hookdb.New()
	err := db.AppendHook([]byte("GAME100#ACT"), func(k, v []byte) (removeHook bool) {
		fmt.Printf("%s..ACTION '%s'!\n", k, v)
		return false
	})

	err = db.Put([]byte("GAME100#ACT1"), []byte("KICK"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("GAME100#ACT2"), []byte("PUNCH"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("GAME100#ACT3"), []byte("GUARD"))
	if err != nil {
		log.Fatal(err)
	}
	// GAME999 !
	err = db.Put([]byte("GAME999#ACT1"), []byte("PUNCH"))
	if err != nil {
		log.Fatal(err)
	}

	// Output:
	// GAME100#ACT1..ACTION 'KICK'!
	// GAME100#ACT2..ACTION 'PUNCH'!
	// GAME100#ACT3..ACTION 'GUARD'!
}

func ExampleDB_AppendHook_removal() {
	db := hookdb.New()
	err := db.AppendHook([]byte("SHOP200#ORDER"), func(k, v []byte) (removeHook bool) {
		fmt.Printf("%s..ORDER '%s'!\n", k, v)
		return true // !
	})

	// exp hit
	err = db.Put([]byte("SHOP200#ORDER1"), []byte("SHOES"))
	if err != nil {
		log.Fatal(err)
	}
	// hook is deleted

	// exp no hit
	err = db.Put([]byte("SHOP200#ORDER2"), []byte("HAT"))
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// SHOP200#ORDER1..ORDER 'SHOES'!
}

func ExampleHookDB_Transaction() {
	db := hookdb.New()
	err := db.Put([]byte("color1"), []byte("red"))
	if err != nil {
		log.Fatal(err)
	}

	txn := db.Transaction()
	v, err := txn.Get([]byte("color1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get color1 in txn: '%s'\n", v)

	// put in txn
	err = txn.Put([]byte("color2"), []byte("blue"))
	if err != nil {
		log.Fatal(err)
	}

	// get in txn
	v, err = txn.Get([]byte("color2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get color2 in txn: '%s'\n", v)

	// get from db
	v, err = db.Get([]byte("color2"))
	if !errors.Is(err, hookdb.ErrKeyNotFound) {
		log.Fatal(err)
	}
	fmt.Printf("get color2 from db: '%s'\n", v)

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	v, err = db.Get([]byte("color2"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get color2 from db after commit: '%s'\n", v)

	// Output:
	// get color1 in txn: 'red'
	// get color2 in txn: 'blue'
	// get color2 from db: ''
	// get color2 from db after commit: 'blue'
}

func ExampleHookDB_AppendHook_withTransaction() {
	db := hookdb.New()

	var count int
	err := db.AppendHook([]byte("pen"), func(k, v []byte) (removeHook bool) {
		fmt.Printf("'%s'\n", v)
		count++
		return false
	})
	if err != nil {
		log.Fatal(err)
	}

	txn := db.Transaction()
	err = txn.Put([]byte("pencil"), []byte("pencil"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1. count='%d' before commit\n", count)

	err = txn.Delete([]byte("pencil"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("2. count='%d' before commit\n", count)

	err = txn.Put([]byte("peninsula"), []byte("peninsula"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("3. count='%d' before commit\n", count)

	// in commit
	// 1. put keys
	// 2. hook called by new keys
	// 3. append new hooks
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1. count='%d' after commit\n", count)

	// Output:
	// 1. count='0' before commit
	// 2. count='0' before commit
	// 3. count='0' before commit
	// 'peninsula'
	// 1. count='1' after commit
}

func ExampleHookDB_TransactionWithLock() {
	db := hookdb.New()

	key := []byte("key")
	txn := db.TransactionWithLock()

	done := make(chan struct{})
	go func() {
		// overwrite
		defer close(done)
		txn := db.TransactionWithLock()
		err := txn.Put(key, []byte("new-val"))
		if err != nil {
			log.Fatal(err)
		}
		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}()
	err := txn.Put(key, []byte("val"))
	if err != nil {
		log.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	<-done
	v, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(v))

	// Output:
	// new-val

}

func ExampleDB_Query() {
	db := hookdb.New()

	err := db.Put([]byte("user01"), []byte("user01"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("user02"), []byte("user02"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("user03"), []byte("user03"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for v, err := range db.Query(ctx, []byte("user")) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(v))
	}

	// Output:
	// user01
	// user02
	// user03

}

func ExampleDB_Query_withReverseQueryOption() {
	db := hookdb.New()

	err := db.Put([]byte("user01"), []byte("user01"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("user02"), []byte("user02"))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put([]byte("user03"), []byte("user03"))
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	for v, err := range db.Query(ctx, []byte("user"), hookdb.WithReverseQuery()) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(v))
	}

	// Output:
	// user03
	// user02
	// user01

}
