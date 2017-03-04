package crc32

import "hash/crc32" // <- using this as reference
import "math/rand"
import "testing"

func TestUpdate(t *testing.T) {
	var in []byte
	//var out, correct uint32
	in = make([]byte, 2*WordBytes*131072)
	fillRand(0, in)
	for _, length := range []int{0, 1, 127, 128, 129, 159, 160, 161, 256, 4097, WordBytes*131072 - 1, WordBytes*131072 + 1, 2 * WordBytes * 131072} {
		t.Log(length)
		out := ChecksumIEEE(in[:length])
		correct := crc32.ChecksumIEEE(in[:length])
		if out != correct {
			t.Fatalf("fail %d %08x %08x\n", length, out, correct)
		}
	}
}

func fillRand(seed uint64, slice []byte) {
	rand.Seed(0)
	for i, _ := range slice {
		slice[i] = uint8(rand.Uint32())
	}
}

const bench_bytes = 16384 + 31

func BenchmarkStdlib(b *testing.B) {
	var in []byte
	in = make([]byte, bench_bytes)
	fillRand(0, in)
	b.SetBytes(bench_bytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		crc32.ChecksumIEEE(in)
	}
}

func BenchmarkKandJ(b *testing.B) {
	var in []byte
	in = make([]byte, bench_bytes)
	fillRand(0, in)
	b.SetBytes(bench_bytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ChecksumIEEE(in)
	}
}
