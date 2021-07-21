package models

type User struct {
	UserName    string `json:"user_name"`
	UserGroup   string `json:"user_group"`
	Role        string `json:"role"`
	CreateTime  string `json:"create_time"`
	ContactInfo string `json:"contact_info"`
	Principal   string `json:"principal"`
	PassWord    string `json:"pass_word"`
}

type UserListReq struct {
	FilterUserName  string `query:"filter_user_name"`
	FilterUserGroup string `query:"filter_user_group"`
}

type UserListResp struct {
	UserList []*User `json:"user_list"`
	BaseResp
}

type CreateOrUpdateUserReq struct {
	UserName    string `json:"user_name" validate:"required"`
	UserGroup   string `json:"user_group" validate:"required"`
	Role        string `json:"role" validate:"required"`
	ContactInfo string `json:"contact_info"`
	Principal   string `json:"principal"`
	PassWord    string `json:"pass_word"`
}

type CreateOrUpdateUserResp struct {
	BaseResp
}

type DeleteUserReq struct {
	UserName  string `form:"user_name" validate:"required"`
	UserGroup string `form:"user_group" validate:"required"`
}

type DeleteUserResp struct {
	BaseResp
}

type CurrentUserResp struct {
	CurrentUser *User `json:"current_user"`
	BaseResp
}
