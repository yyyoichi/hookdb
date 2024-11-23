# HookDB

This key-value store is a simple, lightweight, on-memory database. One of its unique features is the ability to pre-register key prefixes. Whenever a key with a registered prefix is stored in the database, a specific function is triggered to notify or handle that event.

## Features

- Key-Value Store
- Put, Delete, Get and Query commands
- HookHandler, Callback function triggered by put key
- Deletion after HookHandler call
- Transaction
- Scription to key prefix events

## Subscribe to Key Prefix Events

HookDB allows you to subscribe to specific key prefixes and receive notifications whenever a key with the registered prefix is put into the database. This feature is useful for triggering custom logic based on specific key patterns.

## Install

```shell
go get github.com/yyyoichi/hookdb
```

## Example

```golang
import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"

    "github.com/yyyoichi/hookdb"
)

func Example() {
    db := hookdb.New()

    http.HandleFunc("POST /game/{gameId}/actions", func(w http.ResponseWriter, r *http.Request) {
        var requestBody struct {
            Action string `json:"action"`
        }
        _ = json.NewDecoder(r.Body).Decode(&requestBody)

        gameID := r.URL.Query().Get("gameId")
        key := fmt.Sprintf("GAME%s#ACT", gameID)
        err := db.Put([]byte(key), []byte(requestBody.Action))
        if err != nil {
            log.Fatal(err)
        }
        w.WriteHeader(http.StatusOK)
    })

    // in other gorutin, subscribe to the key "GAME100#ACT" until the context is done
    go func() {
        ctx := context.Background()
        event, err := db.Subscribe(ctx, []byte("GAME100#ACT"))
        if err != nil {
            log.Fatal(err)
        }
        for v := range event {
            log.Println(string(v))
        }
    }()

    http.ListenAndServe(":8080", nil)

    // curl -X POST -H "Content-Type: application/json" -d '{"action":"PUNCH"}' http://localhost:8080/game/100/actions
    //  -> print: 'PUNCH'
}

```

[examples](https://pkg.go.dev/github.com/yyyoichi/hookdb#pkg-examples)
