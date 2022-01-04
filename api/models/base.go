package models

type BaseResp struct {
	Message string `json:"message"`
}

func BuildBaseResp(err error) BaseResp {
	if nil == err {
		return BaseResp{
			Message: "ok",
		}
	} else {
		return BaseResp{
			Message: err.Error(),
		}
	}
}
