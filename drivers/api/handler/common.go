package handler

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var NomadHost, ApiAddr, ConsulAddr string

// decodeBody is used to decode a JSON request body
func DecodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

func BuildUrl(path string) string {
	return "http://" + NomadHost + path
}

func InvokeApiWithKvData(method, uri string, args map[string]string, respStruct interface{}) (err error) {
	var req *http.Request
	switch method {
	case http.MethodGet:
		req, err = http.NewRequest(method, uri, nil)
		if err != nil {
			return fmt.Errorf("NewRequest failed: %v", err)
		}
		param := req.URL.Query()
		for k, v := range args {
			param.Add(k, v)
		}
		req.URL.RawQuery = param.Encode()
	case http.MethodPost:
		formData := url.Values{}
		for k, v := range args {
			formData.Add(k, v)
		}
		req, err = http.NewRequest(method, uri, strings.NewReader(formData.Encode()))
		if err != nil {
			return fmt.Errorf("NewRequest failed: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	default:
		return fmt.Errorf("unsupported method: %v", method)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send request failed: %v", err)
	}
	defer resp.Body.Close()
	if err := handleNomadResponse(resp.Body, resp.StatusCode, &respStruct); nil != err {
		return fmt.Errorf("parse response failed: %v", err)
	}

	return nil
}

func InvokePostApiWithJson(url string, reqJson []byte, respStruct interface{}) error {
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqJson))
	if err != nil {
		return fmt.Errorf("invoke nomad api failed, error: %v", err)
	}
	defer resp.Body.Close()
	if err := handleNomadResponse(resp.Body, resp.StatusCode, &respStruct); nil != err {
		return fmt.Errorf("response contains error: %v", err)
	}
	return nil
}

func InvokeHttpUrlWithBody(method, url string, body []byte, respStruct interface{}) error {
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("NewRequest failed: %v", err)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send request failed: %v", err)
	}
	defer resp.Body.Close()
	if err := handleNomadResponse(resp.Body, resp.StatusCode, &respStruct); nil != err {
		return fmt.Errorf("parse response failed: %v", err)
	}

	return nil
}

func handleNomadResponse(respBody io.ReadCloser, statusCode int, respStruct interface{}) error {
	body, err := ioutil.ReadAll(respBody)
	if err != nil {
		return fmt.Errorf("read response body failed, error: %v", err)
	}
	if statusCode/100 != 2 {
		return fmt.Errorf("got error response, statusCode=%v, got response: %v", statusCode, string(body))
	}
	if err := json.Unmarshal(body, &respStruct); nil != err {
		return fmt.Errorf("parse response failed. got response: %v", string(body))
	}
	return nil
}

func rsaDecrypt(ciphertext []byte, privateKey string) ([]byte, error) {
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return nil, fmt.Errorf("private key error!")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf(fmt.Sprintf("Parse private key  error:%s", err))
	}
	return rsa.DecryptPKCS1v15(rand.Reader, priv, ciphertext)
}

func DecryptPasswordSupportNoRsaKey(password string, rsaPrivateKey string) (realPwd string, err error) {
	if "" == rsaPrivateKey {
		return password, nil
	}
	return DecryptPassword(password, rsaPrivateKey)
}

func DecryptPassword(password string, rsaPrivateKey string) (realPwd string, err error) {
	if "" == rsaPrivateKey {
		return "", fmt.Errorf("rsa private key should not be empty")
	}

	realPwdByte := []byte{}
	realPwdByte, err = base64.StdEncoding.DecodeString(password)
	if err != nil {
		return "", err
	}

	realPwdByte, err = rsaDecrypt(realPwdByte, rsaPrivateKey)
	if err != nil {
		return "", err
	}
	realPwd = string(realPwdByte)
	return realPwd, nil
}

func IsEmpty(value interface{}) bool {
	switch value.(type) {
	case string:
		if value.(string) == "" {
			return true
		}
	case int64:
		if value.(int64) == 0 {
			return true
		}
	}
	return false
}
