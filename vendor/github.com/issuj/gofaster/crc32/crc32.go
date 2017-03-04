// Algorithm is from: "Everything we know about CRC but afraid to forget"; Kadatch, A. & Jenkins, M.; 2010
// Also known as the "crcutil" algorithm.
// Code and it's comments are also mostly directly adapted from their pseudocode in that article.

// WARNING: While I understand the theory on why it's faster (splitting the data to N independent
// streams, allowing instruction-level parallelism), I didn't bother to actually figure out the
// math part, i.e. why splitting and combining works.

package crc32

import "hash"
import "unsafe"

//import "fmt"

const D = 32        // Degree of polynomial
const N = 4         // Interleave parameter
const WordBytes = 8 // Size of Word in bytes
const ShiftMask = (WordBytes * 8) - 1
const One = 1 << (D - 1)
const Invert = (1 << D) - 1

type Crc uint32
type Word uint64

type Poly struct {
	MulWordByXpowD            [WordBytes][]Crc
	MulInterleavedWordByXpowD [WordBytes][]Crc
	Pow2n                     [WordBytes * 8]Crc
	polynomial                Crc
}

func (p Poly) Multiply(a, b Crc) Crc {
	var product Crc
	for k := Crc(One); k > 0; k >>= 1 {
		if a&k != 0 {
			product ^= b
		}

		b = (b >> 1) ^ (p.polynomial & -(b & 1))
	}
	return product
}

// returns ((x ** n) mod P).
func (p Poly) XpowN(n uint) Crc {
	var result Crc = One

	for i := 0; n != 0; i, n = i+1, n>>1 {
		if (n & 1) != 0 {
			result = p.Multiply(result, p.Pow2n[i])
		}
	}
	return result
}

// ”v” occupies ”d” least signficant bits.
// ”m” occupies D least significant bits.
func (p Poly) MultiplyUnnormalized(v Crc, d uint, m Crc) Crc {
	var result Crc
	for d > D {
		temp := v & Invert
		v >>= D
		d -= D
		// XpowN returns (x∗∗N mod P(x)).
		result ^= p.Multiply(temp, p.Multiply(m, p.XpowN(d)))
	}
	result ^= p.Multiply(v<<(D-d), m)
	return result
}

func (p Poly) CrcBytes(bytes []byte, v, u Crc) Crc {
	v = v ^ u
	for _, b := range bytes {
		v = (v >> 8) ^ p.MulWordByXpowD[WordBytes-1][byte(v)^b]
	}
	return v ^ u
}

func (p Poly) CrcWord(value Word) Crc {
	var result Crc
	// Unroll this loop or let compiler do it.
	for b := 0; b < WordBytes; b, value = b+1, value>>8 {
		result ^= p.MulWordByXpowD[b][byte(value)]
	}
	return result
}

func crc32_8_4(crc uint32, data []Word, table []Crc) (n int, crc0, crc1, crc2, crc3 uint32)

func (p Poly) CrcInterleavedWordByWord(data []Word, v, u Crc) Crc {
	var crc [N + 1]Crc
	crc[0] = v ^ u
	blocks := len(data) / N
	var i int = 0
	table := p.MulInterleavedWordByXpowD[0][0 : WordBytes*256]
	if D == 32 && WordBytes == 8 && N == 4 { // if on constants
		if blocks-1 > 0 {
			var a, b, c, d uint32
			i, a, b, c, d = crc32_8_4(uint32(crc[0]), data[:N*(blocks-1)], table)
			crc[0], crc[1], crc[2], crc[3] = Crc(a), Crc(b), Crc(c), Crc(d)
		}
	} else {
		var buffer [N]Word
		for ; i < N*(blocks-1); i += N {
			// Load next N words and move overflow bits into ”next” word.
			for n := 0; n < N; n++ {
				buffer[n] = Word(crc[n]) ^ data[i+n]
				if D > WordBytes*8 { // if on constants
					crc[n+1] ^= crc[n] >> (WordBytes * 8)
				}
				crc[n] = 0
			}
			// Compute interleaved word-by-word CRC.
			for base := 0; base < WordBytes*256; base += 256 {
				for n := 0; n < N; n++ {
					buf_n := buffer[n]
					crc[n] ^= table[base+int(byte(buf_n))]
					buffer[n] = buf_n >> 8
				}
			}
			// Combine crc[0] with delayed overflow bits.
			crc[0] ^= crc[N]
			crc[N] = 0
		}
	}

	crc0 := crc[0]
	// Process the last N bytes and combine CRCs.
	if i < N*blocks {
		for n := 0; n < N; n++ {
			if n != 0 {
				crc0 ^= crc[n]
			}
			if D > WordBytes*8 { // if on constants
				crc0 >>= Crc((D - WordBytes*8) & ShiftMask)
				crc0 ^= p.CrcWord(Word(crc0) ^ data[i+n])
			} else {
				crc0 = p.CrcWord(Word(crc0) ^ data[i+n])
			}
		}
		i += N
	}
	for ; i < len(data); i++ {
		crc0 = p.CrcWord(Word(crc0) ^ data[i])
	}
	return (crc0 ^ u)
}

func (p *Poly) InitWordTables() {
	raw := make([]Crc, WordBytes*256)
	for b := 0; b < WordBytes; b++ {
		p.MulWordByXpowD[b] = raw[256*b : 256*(b+1)]
		m := p.XpowN(uint(D + 8*(WordBytes-b-1)))
		for i := 0; i < 256; i++ {
			p.MulWordByXpowD[b][i] = p.MultiplyUnnormalized(Crc(i), 8, m)
			//fmt.Printf("[%d][%d] %08x %08x, %08x\n", b, i, m, p.MulWordByXpowD[b][i], p.MulInterleavedWordByXpowD[b][i])
		}
	}
}

func (p *Poly) InitInterleavedWordTables() {
	raw := make([]Crc, WordBytes*256)
	for b := 0; b < WordBytes; b++ {
		p.MulInterleavedWordByXpowD[b] = raw[256*b : 256*(b+1)]
		m := p.XpowN(uint(D + 8*(N*WordBytes-b-1)))
		for i := 0; i < 256; i++ {
			p.MulInterleavedWordByXpowD[b][i] = p.MultiplyUnnormalized(Crc(i), 8, m)
		}
	}
}

func NewPoly(polynomial Crc) *Poly {
	p := &Poly{}
	p.polynomial = polynomial
	k := Crc(One) >> 1
	for i := 0; i < WordBytes*8; i++ {
		p.Pow2n[i] = k
		k = p.Multiply(k, k)
	}
	p.InitInterleavedWordTables()
	p.InitWordTables()
	return p
}

var IEEE *Poly = NewPoly(0xedb88320)

//var Koopman *Poly = NewPoly(0xeb31d82e)

// Note: Newer x86 CPUs (SSE4.2) have hardware acceleration for calculating CRC32 for
// the Castagnoli polynomial. Go stdlib has support for that, we don't.
//var Castagnoli *Poly = NewPoly(0x82f63b78)

// digest represents the partial evaluation of a checksum.
type digest struct {
	crc Crc
	*Poly
}

func New(p *Poly) hash.Hash32 { return &digest{0, p} }

// NewIEEE creates a new hash.Hash32 computing the CRC-32 checksum
// using the IEEE polynomial.
func NewIEEE() hash.Hash32 { return New(IEEE) }

func (d *digest) Size() int { return 4 }

func (d *digest) BlockSize() int { return 1 }

func (d *digest) Reset() { d.crc = 0 }

func update(crc Crc, poly *Poly, p []byte) Crc {
	const blockSize = 131072
	if len(p) < 128 {
		return poly.CrcBytes(p, crc, Invert)
	}
	nWords := len(p) / WordBytes
	for f := 0; f < nWords; f += blockSize {
		end := blockSize
		if nWords-f < end {
			end = nWords - f
		}
		words := ((*[blockSize]Word)(unsafe.Pointer(&p[f*WordBytes])))[:end]
		crc = poly.CrcInterleavedWordByWord(words, crc, Invert)
	}
	crc = poly.CrcBytes(p[nWords*WordBytes:], crc, Invert)
	return crc
}

// Update returns the result of adding the bytes in p to the crc.
func Update(crc uint32, poly *Poly, p []byte) uint32 {
	return uint32(update(Crc(crc), poly, p))
}

func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = update(d.crc, d.Poly, p)
	return len(p), nil
}

func (d *digest) Sum32() uint32 { return uint32(d.crc) }

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum32()
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

// Checksum returns the CRC-32 checksum of data
// using the polynomial represented by the Table.
func Checksum(data []byte, p *Poly) uint32 { return Update(0, p, data) }

// ChecksumIEEE returns the CRC-32 checksum of data
// using the IEEE polynomial.
func ChecksumIEEE(data []byte) uint32 { return uint32(update(0, IEEE, data)) }
