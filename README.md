# HookDB

This key-value store is a simple, lightweight, on-memory database. One of its unique features is the ability to pre-register key prefixes. Whenever a key with a registered prefix is stored in the database, a specific function is triggered to notify or handle that event.

## Features

- Key-Value Store
- Put, Delete, Get commands
- HookHandler, Callback function triggered by put key
- Deletion after HookHandler call
- Transaction

## Install

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
    err := db.AppendHook([]byte("GAME100#ACT"), func(k, v []byte) (removeHook bool) {
        fmt.Printf("%s: %s\n", k, v)
        return false
    })
    if err != nil {
        log.Fatal(err)
    }

    // exp hit  
    err = db.Put([]byte("GAME100#ACT1"), []byte("PUNCH"))
    if err != nil {
        log.Fatal(err)
    }
    // exp hit
    err = db.Put([]byte("GAME100#ACT2"), []byte("KICK"))
    if err != nil {
        log.Fatal(err)
    }
    // exp not hit
    err = db.Put([]byte("GAME999#ACT1"), []byte("KICK"))
    if err != nil {
        log.Fatal(err)
    }

    // Output:
    // GAME100#ACT1: PUNCH
    // GAME100#ACT2: KICK
}

```

[more...](https://github.com/yyyoichi/hookdb/blob/main/hookdb_test.go)
