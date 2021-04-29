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

var NomadHost string
var ApiAddr string

// decodeBody is used to decode a JSON request body
func DecodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

func BuildUrl(path string) string {
	return "http://" + NomadHost + path
}

func InvokeNomadGetApi(url string, respStruct interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("invoke nomad failed, error: %v", err)
	}
	defer resp.Body.Close()
	if err := handleNomadResponse(resp.Body, resp.StatusCode, &respStruct); nil != err {
		return fmt.Errorf("parse response failed: %v", err)
	}
	return nil
}

func InvokeApiWithFormData(method, uri string, args map[string]string, respStruct interface{}) (err error) {
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

func rsaDecrypt(ciphertext []byte) ([]byte, error) {
	privateKey := `-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQDbJ3VgUDDWy9eQjj7AMS2ShqS03N2GLidAzLUeNhyiXEUVvG/b
0FN/ovYNfOE2ElmNlQybRcamfzmJDnEh/NYnLzCiLn4kYVVUD6/fATJrJrvyIU1l
LTYlHboQSPeGkzQAZbb26WLXYsvVsBJVseFsej8tzyK/ONpOMqIgl+xneQIDAQAB
AoGBALUwonLG2ho83jS95lOwVSVX/MUr9lsBvaJtnTElO/dgoh2edj0euGpGqXft
T6YM9c2A9bNKtTri5QbT0eVvzP2AhqEpZixtDVTA3m/PYzobIdUiJiWV40/WrAtO
hBpygtatUUV7EhqtoQiqSqvhpeO0MmhEVA6LrJZm6lrH7cWhAkEA+DA2nBU6qY9i
fmMzpcKcrDdhVvRLiF5/S3z2tdJeBUzmwQBIweHHLsrC3Pp7ThMImlOjoF2bmW6i
ql5FHtFYOwJBAOINTRHKD9cF7Pb/Gz85yZKdIemg6n3oNrAz8ZLFHIDp0OIYrM+X
CLh3MpiknkWivzhzLC/r3h11kCaOT0XP99sCQQCjdE1i+nBKH87EYl0vfD5nBYos
FHRyeZnog4KQON4HK6CF18QTPLlLzeoMU0NGJi7yRMds5HmH0V98SN3I8CLlAkBu
0RZ3Iheh0cXZUDaLSEkJFv8JCVnrX2tv9gb3bKoMiJNeQ7p0Cha8V7L2Ib11ZdNY
WR3QYFEDIB8Kx7kVAF8BAkAYzk6RZGiGdvp9LeK427qT7hNxhf9zcvEg/0a4fM7M
tMw7KTdfaFIorhu4yOyBNtdfP12JPbWIk5bKHOtZp4+d
-----END RSA PRIVATE KEY-----`
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

func DecryptMysqlPassword(password string) (realPwd string, err error) {
	b, err := base64.StdEncoding.DecodeString(password)
	if err != nil {
		return "", err
	}
	realPwdByte, err := rsaDecrypt(b)
	if err != nil {
		return "", err
	}
	realPwd = string(realPwdByte)
	return realPwd, nil
}