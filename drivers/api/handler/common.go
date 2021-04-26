package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

var NomadHost string

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

func InvokeNomadPostApiWithJson(url string, reqJson []byte, respStruct interface{}) error {
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
