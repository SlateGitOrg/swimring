package ring

import (
	"fmt"
	"testing"
)

func BenchmarkLookup(b *testing.B) {
	for _, nodes := range []int{10, 100, 1000} {
		for _, v := range []int{64, 256} {
			r := MustNew(v, nil, names("b", nodes)...)
			ks := make([]string, 1024)
			for i := range ks {
				ks[i] = fmt.Sprintf("key-%d", i)
			}
			b.Run(fmt.Sprintf("N%d_V%d", nodes, v), func(b *testing.B) {
				i := 0
				for b.Loop() {
					r.Lookup(ks[i&1023])
					i++
				}
			})
		}
	}
}

func BenchmarkLookupN3(b *testing.B) {
	r := MustNew(256, nil, names("b", 100)...)
	i := 0
	for b.Loop() {
		r.LookupN(fmt.Sprint(i&1023), 3)
		i++
	}
}

func BenchmarkAddNode(b *testing.B) {
	r := MustNew(256, nil, names("b", 100)...)
	for b.Loop() {
		if _, err := r.Add("extra"); err != nil {
			b.Fatal(err)
		}
	}
}
