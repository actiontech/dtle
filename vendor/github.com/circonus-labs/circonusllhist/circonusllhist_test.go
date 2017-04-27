package circonusllhist_test

import (
	"math"
	"math/rand"
	"testing"
	"time"

	hist "github.com/circonus-labs/circonusllhist"
)

func helpTestBin(t *testing.T, v float64, val, exp int8) {
	b := hist.NewBinFromFloat64(v)
	if b.Val() != val || b.Exp() != exp {
		t.Errorf("%v -> [%v,%v] expected, but got [%v,%v]", v, val, exp, b.Val(), b.Exp())
	}
}
func fuzzy_equals(expected, actual float64) bool {
	delta := math.Abs(expected / 100000.0)
	if actual >= expected-delta && actual <= expected+delta {
		return true
	}
	return false
}
func TestBins(t *testing.T) {
	helpTestBin(t, 0.0, 0, 0)
	helpTestBin(t, 9.9999e-129, 0, 0)
	helpTestBin(t, 1e-128, 10, -128)
	helpTestBin(t, 1.00001e-128, 10, -128)
	helpTestBin(t, 1.09999e-128, 10, -128)
	helpTestBin(t, 1.1e-128, 11, -128)
	helpTestBin(t, 1e127, 10, 127)
	helpTestBin(t, 9.999e127, 99, 127)
	helpTestBin(t, 1e128, -1, 0)
	helpTestBin(t, -9.9999e-129, 0, 0)
	helpTestBin(t, -1e-128, -10, -128)
	helpTestBin(t, -1.00001e-128, -10, -128)
	helpTestBin(t, -1.09999e-128, -10, -128)
	helpTestBin(t, -1.1e-128, -11, -128)
	helpTestBin(t, -1e127, -10, 127)
	helpTestBin(t, -9.999e127, -99, 127)
	helpTestBin(t, -1e128, -1, 0)
	helpTestBin(t, 9.999e127, 99, 127)
}

func helpTestVB(t *testing.T, v, b, w float64) {
	bin := hist.NewBinFromFloat64(v)
	out := bin.Value()
	interval := bin.BinWidth()
	if out < 0 {
		interval *= -1.0
	}
	if !fuzzy_equals(b, out) {
		t.Errorf("%v -> %v != %v\n", v, out, b)
	}
	if !fuzzy_equals(w, interval) {
		t.Errorf("%v -> [%v] != [%v]\n", v, interval, w)
	}
}
func TestBinSizes(t *testing.T) {
	helpTestVB(t, 43.3, 43.0, 1.0)
	helpTestVB(t, 99.9, 99.0, 1.0)
	helpTestVB(t, 10.0, 10.0, 1.0)
	helpTestVB(t, 1.0, 1.0, 0.1)
	helpTestVB(t, 0.0002, 0.0002, 0.00001)
	helpTestVB(t, 0.003, 0.003, 0.0001)
	helpTestVB(t, 0.3201, 0.32, 0.01)
	helpTestVB(t, 0.0035, 0.0035, 0.0001)
	helpTestVB(t, -1.0, -1.0, -0.1)
	helpTestVB(t, -0.00123, -0.0012, -0.0001)
	helpTestVB(t, -987324, -980000, -10000)
}

var s1 = []float64{0.123, 0, 0.43, 0.41, 0.415, 0.2201, 0.3201, 0.125, 0.13}

func TestDecStrings(t *testing.T) {
	h := hist.New()
	for _, sample := range s1 {
		h.RecordValue(sample)
	}
	out := h.DecStrings()
	expect := []string{"H[0.0e+00]=1", "H[1.2e-01]=2", "H[1.3e-01]=1",
		"H[2.2e-01]=1", "H[3.2e-01]=1", "H[4.1e-01]=2",
		"H[4.3e-01]=1"}
	for i, str := range expect {
		if str != out[i] {
			t.Errorf("DecString '%v' != '%v'", out[i], str)
		}
	}
}

func TestNewFromStrings(t *testing.T) {
	strings := []string{"H[0.0e+00]=1", "H[1.2e-01]=2", "H[1.3e-01]=1",
		"H[2.2e-01]=1", "H[3.2e-01]=1", "H[4.1e-01]=2", "H[4.3e-01]=1"}

	// hist of single set of strings
	singleHist, err := hist.NewFromStrings(strings, false)
	if err != nil {
		t.Error("error creating hist from strings '%v'", err)
	}

	// hist of multiple sets of strings
	strings = append(strings, strings...)
	doubleHist, err := hist.NewFromStrings(strings, false)
	if err != nil {
		t.Error("error creating hist from strings '%v'", err)
	}

	// sanity check the sums are doubled
	if singleHist.ApproxSum()*2 != doubleHist.ApproxSum() {
		t.Error("aggregate histogram approxSum failure")
	}

	if singleHist.Equals(doubleHist) {
		t.Error("histograms should not be equal")
	}
}

func TestMean(t *testing.T) {
	h := hist.New()
	for _, sample := range s1 {
		h.RecordValue(sample)
	}
	mean := h.ApproxMean()
	if !fuzzy_equals(0.2444444444, mean) {
		t.Errorf("mean() -> %v != %v", mean, 0.24444)
	}
}

func helpQTest(t *testing.T, vals, qin, qexpect []float64) {
	h := hist.New()
	for _, sample := range vals {
		h.RecordValue(sample)
	}
	qout, _ := h.ApproxQuantile(qin)
	if len(qout) != len(qexpect) {
		t.Errorf("wrong number of quantiles")
	}
	for i, q := range qout {
		if !fuzzy_equals(qexpect[i], q) {
			t.Errorf("q(%v) -> %v != %v", qin[i], q, qexpect[i])
		}
	}
}
func TestQuantiles(t *testing.T) {
	helpQTest(t, []float64{1}, []float64{0, 0.25, 0.5, 1}, []float64{1, 1.025, 1.05, 1.1})
	helpQTest(t, s1, []float64{0, 0.95, 0.99, 1.0}, []float64{0, 0.4355, 0.4391, 0.44})
	helpQTest(t, []float64{1.0, 2.0}, []float64{0.5}, []float64{1.1})
}

func BenchmarkHistogramRecordValue(b *testing.B) {
	h := hist.NewNoLocks()
	for i := 0; i < b.N; i++ {
		h.RecordValue(float64(i % 1000))
	}
	b.ReportAllocs()
}

func BenchmarkHistogramTypical(b *testing.B) {
	h := hist.NewNoLocks()
	for i := 0; i < b.N; i++ {
		h.RecordValue(float64(i % 1000))
	}
	b.ReportAllocs()
}

func BenchmarkHistogramRecordIntScale(b *testing.B) {
	h := hist.NewNoLocks()
	for i := 0; i < b.N; i++ {
		h.RecordIntScale(i%90+10, (i/1000)%3)
	}
	b.ReportAllocs()
}

func BenchmarkHistogramTypicalIntScale(b *testing.B) {
	h := hist.NewNoLocks()
	for i := 0; i < b.N; i++ {
		h.RecordIntScale(i%90+10, (i/1000)%3)
	}
	b.ReportAllocs()
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		hist.New()
	}
}

func TestConcurrent(t *testing.T) {
	h := hist.New()
	for r := 0; r < 100; r++ {
		go func() {
			for j := 0; j < 100; j++ {
				for i := 50; i < 100; i++ {
					if err := h.RecordValue(float64(i)); err != nil {
						t.Fatal(err)
					}
				}
			}
		}()
	}
}

func TestRang(t *testing.T) {
	h1 := hist.New()
	src := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(src)
	for i := 0; i < 1000000; i++ {
		h1.RecordValue(rnd.Float64() * 10)
	}
}
func TestEquals(t *testing.T) {
	h1 := hist.New()
	for i := 0; i < 1000000; i++ {
		if err := h1.RecordValue(float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	h2 := hist.New()
	for i := 0; i < 10000; i++ {
		if err := h1.RecordValue(float64(i)); err != nil {
			t.Fatal(err)
		}
	}

	if h1.Equals(h2) {
		t.Error("Expected Histograms to not be equivalent")
	}

	h1.Reset()
	h2.Reset()

	if !h1.Equals(h2) {
		t.Error("Expected Histograms to be equivalent")
	}
}
