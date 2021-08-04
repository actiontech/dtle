package models

type UserLoginReqV2 struct {
	Tenant   string `json:"tenant" form:"tenant" example:"test" valid:"required"`
	Username string `json:"username" form:"username" example:"test" valid:"required"`
	Password string `json:"password" form:"password" example:"123456" valid:"required"`
}

type GetUserLoginResV2 struct {
	BaseResp
	Data UserLoginResV2 `json:"data"`
}

type UserLoginResV2 struct {
	Token string `json:"token" example:"this is a jwt token string"`
}
