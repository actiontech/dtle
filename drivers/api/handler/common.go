package handler

import (
	"encoding/json"
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
