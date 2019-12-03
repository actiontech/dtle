package agent

import (
	"net/http"
	"time"

	"github.com/actiontech/dtle/api"
	"github.com/actiontech/dtle/internal/models"
	"github.com/dgrijalva/jwt-go"
)

func (s *HTTPServer) LoginRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "PUT":
		return s.login(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

type jwtCustomClaims struct {
	jwt.StandardClaims

	// 追加自己需要的信息
	userName string `json:"userName"`
	passwd   string `json:"passwd"`
}

/**
 * 生成 token
 * SecretKey 是一个 const 常量
 */
func CreateToken(SecretKey []byte, userName string) (tokenString string, err error) {
	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"foo":  "bar",
		"nbf":  now.Unix(),
		"exp":  now.Add(24 * time.Hour).Unix(),
		"user": userName,
	})
	tokenString, err = token.SignedString(SecretKey)
	return
}

func (s *HTTPServer) login(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var user *api.User
	SecretKey := _HMAC_SECRET
	//user.Canonicalize()
	if err := decodeBody(req, &user); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if user.UserName == "" && user.UserName != "superuser" {
		return nil, CodedError(400, "user not exist")
	}
	if user.Passwd == "" {
		return nil, CodedError(400, "password  err ")
	}

	token, err := CreateToken([]byte(SecretKey), user.UserName)

	var out models.Login
	if err != nil {
		out.Token = token
		out.Code = "500"
		return out, err
	}
	out.Token = token
	out.Code = "200"
	return out, nil
}
