package hookdb

type QueryOptions struct {
	Reverse bool
}
type QueryOption func(*QueryOptions) error

func WithReverseQuery() QueryOption {
	return func(qo *QueryOptions) error {
		qo.Reverse = true
		return nil
	}
}

type SubscribeOptions struct {
	Once    bool
	BufSize *int // default 1
}

func (so *SubscribeOptions) getBufSize() int {
	if so.BufSize == nil {
		return 1
	}
	return *so.BufSize
}

type SubscribeOption func(*SubscribeOptions) error

// WithOnceSubscription returns a SubscribeOption that sets the Once field
// of SubscribeOptions to true, indicating that the subscription should
// only be executed once.
func WithOnceSubscription() SubscribeOption {
	return func(seo *SubscribeOptions) error {
		seo.Once = true
		return nil
	}
}

// WithBufSize sets the buffer size of the channel that the Subscribe function returns.
func WithBufSize(size int) SubscribeOption {
	return func(seo *SubscribeOptions) error {
		seo.BufSize = &size
		return nil
	}
}
