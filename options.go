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
