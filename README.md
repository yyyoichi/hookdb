# hookdb

This key-value store is a simple, lightweight, on-memory database. One of its unique features is the ability to pre-register key prefixes. Whenever a key with a registered prefix is stored in the database, a specific function is triggered to notify or handle that event.

## install

```shell
go get github.com/yyyoichi/hookdb
```

## Example

```golang
import (
    "fmt"
    "log"

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

```

[more...](https://github.com/yyyoichi/hookdb/blob/main/hookdb_test.go)
