package simpledb

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func slist2blist(list []string) (out [][]byte) {
	for _, v := range list {
		out = append(out, []byte(v))
	}
	return out
}

func testMerge(t *testing.T, alist ...[]string) {
	dup := map[string]bool{}
	type kv struct{ key, val []byte }
	var expected []kv

	kl := [][][]byte{}
	vl := [][][]byte{}
	for i, a := range alist {
		k := slist2blist(a)
		kl = append(kl, k)
		v := make([][]byte, len(a))
		for j := range a {
			v[j] = []byte{'A' + byte(i)}
		}
		vl = append(vl, v)

		for j, key := range a {
			if dup[key] {
				continue
			}
			dup[key] = true
			expected = append(expected, kv{k[j], v[j]})
		}
	}

	slices.SortStableFunc(expected, func(a, b kv) int {
		return bytes.Compare(a.key, b.key)
	})

	var seqs []SortedKV
	for i, k := range kl {
		arr := &SortedArray{}
		arr.keys = k
		arr.vals = vl[i]
		arr.deleted = nil
		seqs = append(seqs, arr)
	}
	m := MergedSortedKV(seqs)

	i := 0
	iter, err := m.Iter()
	require.Nil(t, err)
	for ; err == nil && iter.Valid(); err = iter.Next() {
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())
		i++
	}
	require.Nil(t, err)
	assert.False(t, iter.Valid())
	assert.Equal(t, len(expected), i)

	for err = iter.Prev(); err == nil && iter.Valid(); err = iter.Prev() {
		i--
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())
	}
	require.Nil(t, err)
	assert.False(t, iter.Valid())
	assert.Equal(t, 0, i)

	for ; err == nil && iter.Valid(); err = iter.Next() {
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())

		err = iter.Prev()
		require.Nil(t, err)
		i--
		assert.Equal(t, expected[i].key, iter.Key())
		assert.Equal(t, expected[i].val, iter.Val())

		err = iter.Next()
		require.Nil(t, err)
		i += 2
	}
}

func TestMerge(t *testing.T) {
	a := []string{}
	b := []string{}
	testMerge(t, a, b)
	a = []string{"apple", "cherry"}
	b = []string{}
	testMerge(t, a, b)
	a = []string{}
	b = []string{"apple", "cherry"}
	testMerge(t, a, b)
	a = []string{"apple", "cherry"}
	b = []string{"apple", "cherry"}
	testMerge(t, a, b)
	a = []string{"apple", "cherry"}
	b = []string{"berry", "date"}
	testMerge(t, a, b)
	a, b = b, a
	testMerge(t, a, b)
}
