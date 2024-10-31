package hookdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestL2HookStore(t *testing.T) {
	l2 := l2hookStore{l1Store: newL1Store[HookHandler]()}
	test := []string{
		"a", "ab", "abc", "abcd", "abcde", "b", "bc",
	}
	called := make([]string, 0, len(test))
	for _, tt := range test {
		_, err := l2.Exec(l2.put, input[HookHandler]{k: []byte(tt), v: func(k, v []byte) (removeHook bool) {
			called = append(called, tt)
			return false
		}})
		assert.NoError(t, err)
	}

	for output, err := range l2.FoundPrefix([]byte("abcd!")) {
		assert.NoError(t, err)
		output.val(nil, nil)
	}

	assert.Equal(t, []string{"abcd", "abc", "ab", "a"}, called)
}
