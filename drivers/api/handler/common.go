package handler

import (
	"encoding/json"
	"fmt"
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
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body failed, error: %v", err)
	}
	if err := json.Unmarshal(body, &respStruct); nil != err {
		return fmt.Errorf("got response: %v", string(body))
	}
	return nil
}
