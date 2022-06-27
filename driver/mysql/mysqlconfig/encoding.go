/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysqlconfig

import (
	"fmt"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
	"golang.org/x/text/transform"
)

var charsetEncodingMap = map[string]encoding.Encoding{
	"latin1": charmap.Windows1252,
	"latin2": charmap.Windows1250,
	"gbk": simplifiedchinese.GBK,
	"gb2312": simplifiedchinese.GB18030,
	"gb18030": simplifiedchinese.GB18030,
	"utf16": unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM),
	"utf16le": unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM),
	"utf32": utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM),
}

// return original string and error if charset is not recognized
func ConvertToUTF8(s string, charset string) (string, error) {
	enc, ok := charsetEncodingMap[charset]
	if !ok {
		return s, fmt.Errorf("unknown character set %v", charset)
	}
	r, _, err := transform.String(enc.NewDecoder(), s)
	if err != nil {
		return "", err
	}
	return r, nil
}
func ConvertFromUTF8(s string, charset string) (string, error) {
	enc, ok := charsetEncodingMap[charset]
	if !ok {
		return "", fmt.Errorf("unknown character set %v", charset)
	}
	r, _, err := transform.String(enc.NewEncoder(), s)
	if err != nil {
		return "", err
	}
	return r, nil
}
