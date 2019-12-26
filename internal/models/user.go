package models

type Login struct {
	Token   string
	Code    string
	Message string
}
type Verify struct {
	Image     string
	Code      string
	CaptchaId string
	Message   string
}
