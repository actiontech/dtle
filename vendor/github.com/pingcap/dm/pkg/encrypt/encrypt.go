// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encrypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	secretKey, _ = hex.DecodeString("a529b7665997f043a30ac8fadcb51d6aa032c226ab5b7750530b12b8c1a16a48")
	ivSep        = []byte("@") // ciphertext format: iv + ivSep + encrypted-plaintext
)

// SetSecretKey sets the secret key which used to encrypt.
func SetSecretKey(key []byte) error {
	switch len(key) {
	case 16, 24, 32:
		break
	default:
		return terror.ErrEncryptSecretKeyNotValid.Generate(len(key))
	}
	secretKey = key
	return nil
}

// Encrypt encrypts plaintext to ciphertext.
func Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, terror.ErrEncryptGenCipher.Delegate(err)
	}

	iv, err := genIV(block.BlockSize())
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, 0, len(iv)+len(ivSep)+len(plaintext))
	ciphertext = append(ciphertext, iv...)
	ciphertext = append(ciphertext, ivSep...)
	ciphertext = append(ciphertext, plaintext...) // will be overwrite by XORKeyStream

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[len(iv)+len(ivSep):], plaintext)

	return ciphertext, nil
}

// Decrypt decrypts ciphertext to plaintext.
func Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < block.BlockSize()+len(ivSep) {
		return nil, terror.ErrCiphertextLenNotValid.Generate(block.BlockSize()+len(ivSep), len(ciphertext))
	}

	if !bytes.Equal(ciphertext[block.BlockSize():block.BlockSize()+len(ivSep)], ivSep) {
		return nil, terror.ErrCiphertextContextNotValid.Generate()
	}

	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize()+len(ivSep):]
	plaintext := make([]byte, len(ciphertext))

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, nil
}

func genIV(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, terror.ErrEncryptGenIV.Delegate(err)
}
