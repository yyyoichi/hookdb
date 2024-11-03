package hookdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestL2Query(t *testing.T) {
	l2 := l2valueStore{l1Store: newL1Store[[]byte]()}
	test := []string{
		"a", "ab", "abc", "abcd", "abcde", "b", "bc",
	}
	for _, tt := range test {
		_, err := l2.Exec(l2.put, input[[]byte]{k: []byte(tt), v: []byte("val")})
		assert.NoError(t, err)
	}

	var count int
	for output, err := range l2.Query(context.Background(), []byte("abc")) {
		assert.NoError(t, err)
		count++
		switch count {
		case 1:
			assert.Equal(t, "abc", string(output.key))
		case 2:
			assert.Equal(t, "abcd", string(output.key))
		case 3:
			assert.Equal(t, "abcde", string(output.key))
		}
	}
	assert.Equal(t, 3, count)
}

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
