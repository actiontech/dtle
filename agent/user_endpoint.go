package agent

import (
	"encoding/base64"
	"log"
	"net/http"
	"time"

	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"encoding/json"

	"github.com/actiontech/dtle/api"
	"github.com/actiontech/dtle/internal/models"
	"github.com/dgrijalva/jwt-go"
	"github.com/mojocn/base64Captcha"
)

func (s *HTTPServer) LoginRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "post":
		return s.login(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}
func (s *HTTPServer) VerifyCodeRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "get":
		return s.generateCaptchaHandler(resp, req)
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

//configJsonBody json request body.
type configJsonBody struct {
	Id            string
	CaptchaType   string
	VerifyValue   string
	DriverAudio   *base64Captcha.DriverAudio
	DriverString  *base64Captcha.DriverString
	DriverChinese *base64Captcha.DriverChinese
	DriverMath    *base64Captcha.DriverMath
	DriverDigit   *base64Captcha.DriverDigit
}

var store = base64Captcha.DefaultMemStore

func (s *HTTPServer) generateCaptchaHandler(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	//parse request parameters
	decoder := json.NewDecoder(r.Body)
	var param configJsonBody
	err := decoder.Decode(&param)
	if err != nil {
		log.Println(err)
	}
	defer r.Body.Close()
	var driver base64Captcha.Driver

	//create base64 encoding captcha
	switch param.CaptchaType {
	case "audio":
		driver = param.DriverAudio
	case "string":
		driver = param.DriverString.ConvertFonts()
	case "math":
		driver = param.DriverMath.ConvertFonts()
	case "chinese":
		driver = param.DriverChinese.ConvertFonts()
	default:
		driver = param.DriverDigit
	}
	c := base64Captcha.NewCaptcha(driver, store)
	id, b64s, err := c.Generate()
	body := map[string]interface{}{"code": 1, "data": b64s, "captchaId": id, "msg": "success"}
	if err != nil {
		body = map[string]interface{}{"code": 0, "msg": err.Error()}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(body)
	return body, nil
}

func RsaDecrypt(ciphertext []byte) ([]byte, error) {
	privateKey := ""
	block, _ := pem.Decode([]byte(privateKey))
	if block == nil {
		return nil, CodedError(400, "private key error!")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, CodedError(400, fmt.Sprintf("Parse private key  error:%s", err))
	}
	return rsa.DecryptPKCS1v15(rand.Reader, priv, ciphertext)
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
	if user.VarifyCode == "" {
		return nil, CodedError(400, "VarifyCode  err ")
	}

	encryptPwd := user.Passwd
	b, err := base64.StdEncoding.DecodeString(encryptPwd)
	if err != nil {
		//....
	}
	realPasswd, err := RsaDecrypt(b)
	if err != nil {
		//.....
	}

	/*	decodedPwd, err := base64.StdEncoding.DecodeString(user.Passwd)
		if err != nil {
			return nil, CodedError(400, "password  err ")
		}
		decodestr := string(decodedPwd)

		strs := strings.Split(decodestr, "|")
		int, err := strconv.Atoi(strs[1])
		if err != nil {
			return nil, CodedError(400, "password  err ")
		}
		realPasswd := strs[0][0:int] + strs[0][int+5:]*/

	if realPasswd != nil {
		return nil, CodedError(400, "password  err A12c8Tio$i567onx8@w ")
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
