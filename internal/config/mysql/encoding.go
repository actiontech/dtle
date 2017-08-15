package mysql

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
	charsetEncodingMap["gbk"] = simplifiedchinese.GBK
	charsetEncodingMap["gb2312"] = simplifiedchinese.HZGB2312
}
