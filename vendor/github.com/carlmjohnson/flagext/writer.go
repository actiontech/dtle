package flagext

import (
	"flag"
	"fmt"
	"io"
	"os"
)

// Writer is an io.WriteCloser that can be set as a flag.Value
type Writer interface {
	io.WriteCloser
	flag.Getter
}

type writer struct {
	path string
	file *os.File
}

// FileWriter returns an io.WriteCloser that lazily os.Creates a file set as a flag.Value.
// Pass StdIO ("-") to write to standard output.
func FileWriter(defaultPath string) Writer {
	return &writer{path: defaultPath}
}

func (w *writer) Get() interface{} {
	return w
}

func (w *writer) Set(val string) error {
	w.path = val
	return nil
}

func (w *writer) String() string {
	if w.path == StdIO {
		return "stdout"
	}
	return w.path
}

func (w *writer) Write(p []byte) (n int, err error) {
	if w.file == nil {
		if err := w.init(); err != nil {
			return 0, err
		}
	}

	return w.file.Write(p)
}

func (w *writer) Close() error {
	if w.file == nil {
		return nil
	}
	return w.file.Close()
}

func (w *writer) init() error {
	if w.path == "" {
		return fmt.Errorf("no path set")
	}
	if w.path == StdIO {
		w.file = os.Stdout
		return nil
	}
	f, err := os.Create(w.path)
	if err == nil {
		w.file = f
	}
	return err
}
