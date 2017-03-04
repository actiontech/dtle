package base64

import "encoding/base64" // <- using this as reference
import "math/rand"       // <- random bytes
import "testing"

//import "fmt"

func fillRand(seed uint64, slice []byte) {
	rand.Seed(0)
	for i, _ := range slice {
		slice[i] = uint8(rand.Uint32())
	}
}

func TestBase64Raw(t *testing.T) {
	var in, out, correct, alphabet []byte
	in = make([]byte, 258)
	out = make([]byte, 344)
	correct = make([]byte, 344)
	alphabet = []byte(base64StdAlphabet)
	for i := 0; i < 256; i++ {
		in[i] = uint8(i)
	}
	read, wrote := base64_enc(out, in, alphabet)
	base64.StdEncoding.Encode(correct, in)
	if read != 258 {
		t.Fatal("Wrong amount read", read)
	}
	if wrote != 344 {
		t.Fatal("Wrong amount written", read)
	}
	// Verify the input buffer is unchanged
	for i, v := range in {
		if i < 256 {
			if v != uint8(i) {
				t.Fatal("Input changed at offset", i)
			}
		} else {
			if v != 0 {
				t.Fatal("Input changed at offset", i)
			}
		}
	}
	for i, v := range out {
		if v != correct[i] {
			t.Log(string(out))
			t.Log(string(correct))
			t.Fatal("Wrong output at byte", i)
		}
	}
}

func TestEncode(t *testing.T) {
	var in, out, correct []byte
	for n := 0; n < 32; n++ {
		length := int(rand.Uint32() % (4096 * 128))
		in = make([]byte, length)
		out = make([]byte, StdEncoding.EncodedLen(length))
		correct = make([]byte, StdEncoding.EncodedLen(length))
		fillRand(uint64(n), in)
		StdEncoding.Encode(out, in)
		base64.StdEncoding.Encode(correct, in)
		for i, v := range out {
			if v != correct[i] {
				t.Fatal("Wrong output at byte", i)
			}
		}
	}
}

const bench_b64_words = 16384

func BenchmarkStdlib(b *testing.B) {
	var in, out []byte
	in = make([]byte, bench_b64_words*3)
	out = make([]byte, bench_b64_words*4)
	fillRand(0, in)
	b.SetBytes(bench_b64_words * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base64.StdEncoding.Encode(out, in)
	}
}

func BenchmarkSimd(b *testing.B) {
	var in, out []byte
	in = make([]byte, bench_b64_words*3)
	out = make([]byte, bench_b64_words*4)
	fillRand(0, in)
	b.SetBytes(bench_b64_words * 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StdEncoding.Encode(out, in)
	}
}
