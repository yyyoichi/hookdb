package hookdb

type QueryOptions struct {
	Reverse bool
}
type QueryOption func(*QueryOptions)

func WithReverseQuery() QueryOption {
	return func(qo *QueryOptions) {
		qo.Reverse = true
	}
}
