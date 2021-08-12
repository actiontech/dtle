package models

type UserLoginReqV2 struct {
	Tenant    string `json:"tenant" form:"tenant" example:"test" valid:"required"`
	Username  string `json:"username" form:"username" example:"test" valid:"required"`
	Password  string `json:"password" form:"password" example:"123456" valid:"required"`
	Captcha   string `json:"captcha" form:"captcha" example:"01722" valid:"required"`
	CaptchaId string `json:"captcha_id" form:"captcha_id" example:"Md9kzZQn9xohumhOTc81" valid:"required"`
}

type GetUserLoginResV2 struct {
	BaseResp
	Data UserLoginResV2 `json:"data"`
}

type UserLoginResV2 struct {
	Token string `json:"token" example:"this is a jwt token string"`
}

//VerifyCodeReqV2 json request body.
type VerifyCodeReqV2 struct {
	CaptchaType string `form:"captcha_type"`
}

type CaptchaRespV2 struct {
	Id         string `json:"id"`
	DataScheme string `json:"data_scheme"`
	BaseResp
}
