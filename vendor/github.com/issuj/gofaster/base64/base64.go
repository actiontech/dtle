package base64

import "io"
import "errors"
import stdb64 "encoding/base64"

func base64_enc(dst, src []byte, code []byte) (read, written uint64)

const base64StdAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
const base64UrlAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

type Encoding struct {
	alphabet []byte
	baseImpl *stdb64.Encoding
}

// Similar to standard encoding/base64.*Encoding.Encode()
func (enc *Encoding) Encode(dst, src []byte) {
	read, wrote := base64_enc(dst, src, enc.alphabet)
	dst = dst[wrote:]
	src = src[read:]
	if len(src) > 0 {
		enc.baseImpl.Encode(dst, src)
	}
}

// EncodeToString returns the base64 encoding of src.
func (enc *Encoding) EncodeToString(src []byte) string {
	buf := make([]byte, enc.baseImpl.EncodedLen(len(src)))
	enc.Encode(buf, src)
	return string(buf)
}

func (enc *Encoding) Decode(dst, src []byte) (n int, err error) {
	return enc.baseImpl.Decode(dst, src)
}

// DecodeString returns the bytes represented by the base64 string s.
func (enc *Encoding) DecodeString(s string) ([]byte, error) {
	return enc.baseImpl.DecodeString(s)
}

func (enc *Encoding) EncodedLen(n int) int { return enc.baseImpl.EncodedLen(n) }

type encoder struct {
	err  error
	enc  *Encoding
	w    io.Writer
	ibuf []byte // buffered data waiting to be encoded
	obuf []byte // output buffer
}

var StdEncoding = &Encoding{[]byte(base64StdAlphabet), stdb64.StdEncoding}
var URLEncoding = &Encoding{[]byte(base64UrlAlphabet), stdb64.URLEncoding}

var ErrWriterClosed = errors.New("the Writer is Closed")
var ErrTooSmallBuffer = errors.New("too small buffer, at least 1 word required")

func NewEncoder(enc *Encoding, w io.Writer) io.WriteCloser {
	return NewEncoderSize(enc, w, 4096)
}

func NewEncoderSize(enc *Encoding, w io.Writer, bufWords int) io.WriteCloser {
	ret := &encoder{enc: enc, w: w, ibuf: make([]byte, 0, bufWords*3), obuf: make([]byte, 0, bufWords*4)}
	if bufWords < 1 {
		ret.err = ErrTooSmallBuffer
	}
	return ret
}

func (e *encoder) Write(p []byte) (n int, err error) {
	if e.err != nil {
		return 0, e.err
	}

	for len(p) > 0 {
		if len(e.ibuf) == 0 && len(p) >= cap(e.ibuf) {
			// large write, nothing in-buffer -> direct encode without copying
			e.obuf = e.obuf[:cap(e.obuf)]
			e.enc.Encode(e.obuf, p[:cap(e.ibuf)])
			p = p[cap(e.ibuf):]
			n += cap(e.ibuf)
		} else {
			spaceAvailable := (cap(e.ibuf) - len(e.ibuf))
			toMove := len(p)
			if spaceAvailable < toMove {
				toMove = spaceAvailable
			}
			e.ibuf = append(e.ibuf, p[:toMove]...)
			n += toMove
			p = p[toMove:]
			if len(e.ibuf) == cap(e.ibuf) {
				e.obuf = e.obuf[:cap(e.obuf)]
				e.enc.Encode(e.obuf, e.ibuf)
				e.ibuf = e.ibuf[:0]
			}
		}
		if len(e.obuf) == cap(e.obuf) {
			if _, e.err = e.w.Write(e.obuf); e.err != nil {
				return n, e.err
			}
			e.obuf = e.obuf[0:0]
		}
	}
	return n, e.err
}

func (e *encoder) Close() error {
	if e.err != nil {
		return e.err
	}
	var err error
	if len(e.ibuf) > 0 {
		outlen := e.enc.EncodedLen(len(e.ibuf))
		e.obuf = e.obuf[0:outlen]
		e.enc.Encode(e.obuf, e.ibuf)
		_, err = e.w.Write(e.obuf)
	}
	if err != nil {
		e.err = err
		return e.err
	}
	e.err = ErrWriterClosed
	return nil
}
