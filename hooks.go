package hookdb

import (
	"context"
	"sync"
)

// Subscribe subscribes to events with the given prefix and sends the data to the returned channel.
// If default option is used, the returned channel will not close until the provided context is done.
func (db *DB) Subscribe(ctx context.Context, prefix []byte, opts ...SubscribeOption) (<-chan []byte, error) {
	var so SubscribeOptions
	for _, opt := range opts {
		_ = opt(&so)
	}

	var p = pipe{
		ch: make(chan []byte),
	}
	var ch = make(chan []byte, so.getBufSize())
	err := db.AppendHook(prefix, func(k, v []byte) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			p.recieve(v)
		}
		return false
	})
	if err != nil {
		p.close()
		close(ch)
		return nil, err
	}
	go func() {
		defer func() {
			p.close()
			close(ch)
			_ = db.RemoveHook(prefix)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-p.ch:
				if !ok {
					return
				}
				ch <- v
			}
			if so.Once {
				return
			}
		}
	}()
	return ch, nil
}

type pipe struct {
	ch chan []byte

	done bool
	mu   sync.RWMutex // mu for done
}

func (p *pipe) recieve(v []byte) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.done {
		return
	}
	p.ch <- v
}

func (p *pipe) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return
	}
	p.done = true
	close(p.ch)
}
