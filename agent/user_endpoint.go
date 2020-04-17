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

	"strings"

	"github.com/actiontech/dts/api"
	"github.com/actiontech/dts/internal/models"
	"github.com/mojocn/base64Captcha"
	"github.com/dgrijalva/jwt-go"
)

func (s *HTTPServer) LoginRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "POST":
		return s.login(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}
func (s *HTTPServer) VerifyCodeRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "POST":
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
//ConfigJsonBody json request body.
type ConfigJsonBody struct {
	Id              string
	CaptchaType     string
	VerifyValue     string
	ConfigAudio     base64Captcha.ConfigAudio
	ConfigCharacter base64Captcha.ConfigCharacter
	ConfigDigit     base64Captcha.ConfigDigit
}

var configC = base64Captcha.ConfigCharacter{
	Height:             60,
	Width:              240,
	Mode:               0,
	ComplexOfNoiseText: 0,
	ComplexOfNoiseDot:  0,
	IsShowHollowLine:   false,
	IsShowNoiseDot:     false,
	IsShowNoiseText:    false,
	IsShowSlimeLine:    false,
	IsShowSineLine:     false,
	CaptchaLen:         6,
}

func (s *HTTPServer) generateCaptchaHandler(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	//parse request parameters
	decoder := json.NewDecoder(r.Body)
	var postParameters ConfigJsonBody
	err := decoder.Decode(&postParameters)
	if err != nil {
		log.Println(err)
	}
	defer r.Body.Close()

	//create base64 encoding captcha
	//创建base64图像验证码

	var config interface{}
	switch postParameters.CaptchaType {
	case "audio":
		config = postParameters.ConfigAudio
	case "character":
		config = postParameters.ConfigCharacter
	default:
		config = postParameters.ConfigDigit
	}
	config = configC
	//GenerateCaptcha 第一个参数为空字符串,包会自动在服务器一个随机种子给你产生随机uiid.
	captchaId, digitCap := base64Captcha.GenerateCaptcha(postParameters.Id, config)
	base64Png := base64Captcha.CaptchaWriteToBase64Encoding(digitCap)

	//or you can do this
	//你也可以是用默认参数 生成图像验证码
	//base64Png := captcha.GenerateCaptchaPngBase64StringDefault(captchaId)

	//set json response
	//设置json响应
	var out models.Verify

	out.Message = "success"
	out.Code = "200"
	out.CaptchaId = captchaId
	out.Image = base64Png
	return out, nil

}

func RsaDecrypt(ciphertext []byte) ([]byte, error) {
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
	if user.VarifyCode == "" {
		return nil, CodedError(400, " {\"code\": \"001\",\"message\": \" VarifyCode  error \"}")
	}
	verifyResult := base64Captcha.VerifyCaptcha(user.VerifyId, user.VarifyCode)
	if !verifyResult {
		return nil, CodedError(400, " {\"code\": \"001\",\"message\": \" VarifyCode  error \"}")
	}
	if user.UserName == "" {
		return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
	}
	if user.Passwd == "" {
		return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
	}

	encryptPwd := user.Passwd
	b, err := base64.StdEncoding.DecodeString(encryptPwd)
	if err != nil {
		return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
	}
	realPasswd, err := RsaDecrypt(b)
	if err != nil {
		return nil, CodedError(400, err.Error())
	}
	if user.UserName == "superuser" {
		if string(realPasswd) != "A12c8Tio$i567onx8@w" {
			return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
		}
	} else {
		args := models.UserSpecificRequest{
			UserID: user.UserName,
		}
		s.parseRegion(req, &args.Region)

		var out models.SingleUserResponse
		if err := s.agent.RPC("User.GetUser", &args, &out); err != nil {
			return nil, err
		}
		if out.User == nil {
			return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
		}
		if out.User.Passwd != string(realPasswd) {
			return nil, CodedError(400, " {\"code\": \"002\",\"message\": \"passwd or username error\"}")
		}
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

func (s *HTTPServer) UserAddRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "POST":
		return s.userAdd(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}
func (s *HTTPServer) UserEditRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "POST":
		return s.userEdit(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) UserListRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "GET":
		path := strings.TrimPrefix(req.URL.Path, "/v1/user/list/")
		return s.userList(resp, req, path)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) UserDeleteRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "DELETE":
		path := strings.TrimPrefix(req.URL.Path, "/v1/user/delete/")
		return s.UserDelete(resp, req, path)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

type CloudUserResponse struct {
	success bool
	code    string
	err     string
}

func (s *HTTPServer) userAdd(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var user *api.AddUser
	if err := decodeBody(req, &user); err != nil {
		return nil, CodedError(400, err.Error())
	}
	args := user
	if args.UserName == "" {
		return nil, CodedError(400, " {\"code\": \"002\",\"success\": \" false  \",\"err\": \" user not null \"}")
	}
	if args.Passwd == "" {
		return nil, CodedError(400, " {\"code\": \"003\",\"success\": \" false  \",\"err\": \" passwd not null \"}")
	}
	if args.Phone == "" {
		return nil, CodedError(400, " {\"code\": \"003\",\"success\": \" false  \",\"err\": \" phone not null \"}")
	}

	encryptPwd := args.Passwd
	pwd, err := base64.StdEncoding.DecodeString(encryptPwd)
	if err != nil {
		return nil, CodedError(400, " {\"code\": \"003\",\"success\": \" false  \",\"err\": \" decode pwd  err \"}")
	}
	realPasswd, err := RsaDecrypt(pwd)
	if err != nil {
		return nil, CodedError(400, err.Error())
	}
	args.Passwd = string(realPasswd)
	verifyArgs := models.UserSpecificRequest{
		UserID: user.UserName,
	}
	s.parseRegion(req, &verifyArgs.Region)

	var verify models.SingleUserResponse
	if err := s.agent.RPC("User.GetUser", &verifyArgs, &verify); err != nil {
		return nil, err
	}
	if verify.User != nil {
		return nil, CodedError(400, " {\"code\": \"005\",\"success\": \" false  \",\"err\": \" user exist \"}")
	}

	sUser := ApiUserToStructUser(args)

	regReq := models.UserRegisterRequest{
		User: sUser,
		WriteRequest: models.WriteRequest{
			Region: "global",
		},
	}
	var rsp models.UserResponse
	var out models.CloudUserResponse
	err = s.agent.RPC("User.Register", &regReq, &rsp)
	if err != nil {
		return nil, err
	}

	if rsp.Success {
		out.Success = true
		out.Code = "200"

	}
	return out, nil

}

func ApiUserToStructUser(user *api.AddUser) *models.User {

	usr := &models.User{
		UserName:   user.UserName,
		Passwd:     user.Passwd,
		Phone:      user.Phone,
		ID:         user.UserName,
		CreateDate: time.Now(),
		UpdateDate: time.Now(),
	}
	return usr
}

func (s *HTTPServer) userEdit(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var user *api.EditUser
	if err := decodeBody(req, &user); err != nil {
		return nil, CodedError(400, err.Error())
	}
	args := user
	if args.UserName == "" {
		return nil, CodedError(400, " {\"code\": \"002\",\"success\": \" false  \",\"err\": \" user not null \"}")
	}
	if args.Passwd == "" {
		return nil, CodedError(400, " {\"code\": \"003\",\"success\": \" false  \",\"err\": \" passwd not null \"}")
	}
	if args.Phone == "" {
		return nil, CodedError(400, " {\"code\": \"009\",\"success\": \" false  \",\"err\": \" phone not null \"}")
	}

	encryptPwd := args.Passwd
	pwd, err := base64.StdEncoding.DecodeString(encryptPwd)
	if err != nil {
		return nil, CodedError(400, " {\"code\": \"008\",\"success\": \" false  \",\"err\": \" decode pwd  err \"}")
	}
	realPasswd, err := RsaDecrypt(pwd)
	if err != nil {
		return nil, CodedError(400, err.Error())
	}
	args.Passwd = string(realPasswd)
	verifyArgs := models.UserSpecificRequest{
		UserID: user.UserID,
	}
	s.parseRegion(req, &verifyArgs.Region)

	var verify models.SingleUserResponse
	if err := s.agent.RPC("User.GetUser", &verifyArgs, &verify); err != nil {
		return nil, err
	}
	if verify.User == nil {
		return nil, CodedError(400, " {\"code\": \"005\",\"success\": \" false  \",\"err\": \" user not exist \"}")

	}
	if args.UserID!="superuser" {
		if verify.User.ID!=args.UserID{
			return nil, CodedError(400, " {\"code\": \"007\",\"success\": \" false  \",\"err\": \" user only change himself\"}")
		}
	}
	if verify.User.UserName!=args.UserName{
		return nil, CodedError(400, " {\"code\": \"009\",\"success\": \" false  \",\"err\": \" username not change \"}")
	}
	if verify.User.Phone!=args.Phone{
		return nil, CodedError(400, " {\"code\": \"010\",\"success\": \" false  \",\"err\": \" Phone not change \"}")
	}
    args.CreateDate = verify.User.CreateDate
	sUser := ApiUserToStructEditUser(args)

	regReq := models.UserRegisterRequest{
		User: sUser,
		WriteRequest: models.WriteRequest{
			Region: "global",
		},
	}
	var rsp models.UserResponse
	var out models.CloudUserResponse

	if err := s.agent.RPC("User.Register", &regReq, &rsp); err != nil {
		return nil, err
	}
	if rsp.Success {
		out.Success = true
		out.Code = "200"
	}
	return out, nil

}

func ApiUserToStructEditUser(user *api.EditUser) *models.User {

	usr := &models.User{
		UserName:   user.UserName,
		Passwd:     user.Passwd,
		Phone:      user.Phone,
		ID:         user.UserID,
		CreateDate: user.CreateDate,
		UpdateDate: time.Now(),
	}
	return usr
}

func (s *HTTPServer) UserDelete(resp http.ResponseWriter, req *http.Request,
	userId string) (interface{}, error) {
	args := models.UserDeregisterRequest{
		UserID: userId,
	}
	s.parseRegion(req, &args.Region)

	var value models.UserResponse
	if err := s.agent.RPC("User.Deregister", &args, &value); err != nil {
		return nil, err
	}
	setIndex(resp, value.Index)
	var out models.CloudUserResponse

	if value.Success {
		out.Success = true
		out.Code = "200"
	}
	return out, nil
}
func (s *HTTPServer) userList(resp http.ResponseWriter, req *http.Request,userId string) (interface{}, error) {

	args := models.UserListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.UserListResponse
	var reqOut models.PageUserListResponse
	if err := s.agent.RPC("User.List", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Users == nil&& userId!="superuser" {
		return nil, CodedError(404, "user not found")
	}
	loc,_:=time.LoadLocation("Asia/Shanghai")
	for i := 0; i < len(out.Users); i++ {
		if userId=="superuser"{
			reqOut.Users =  append(reqOut.Users, *out.Users[i])
			reqOut.Users[i].Passwd="******"
		}else if out.Users[i].ID==userId{
			reqOut.Users =  append(reqOut.Users, *out.Users[i])
			reqOut.Users[0].Passwd="******"
		}
		if !reqOut.Users[i].UpdateDate.IsZero(){
			reqOut.Users[i].UpdateDate,_=time.ParseInLocation( "2006-01-02 15:04:05",reqOut.Users[i].UpdateDate.Add(8*time.Hour).Format("2006-01-02 15:04:05"),loc)
		}
	}
	if  userId=="superuser" {
	user:=models.User{
		ID:"superuser",
		Passwd : "******",
		UserName:"superuser",
		UpdateDate:time.Unix(1584337360, 0).In(loc),
	}
	reqOut.Users =  append(reqOut.Users, user)
	}
	return reqOut.Users, nil
}
