package mysql

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
}
