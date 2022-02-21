/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysqlconfig

import (
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode/utf32"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
	charsetEncodingMap["latin2"] = charmap.Windows1250
	charsetEncodingMap["gbk"] = simplifiedchinese.GBK
	charsetEncodingMap["gb2312"] = simplifiedchinese.GB18030
	charsetEncodingMap["utf32"] = utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM)
}
