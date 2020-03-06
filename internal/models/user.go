package models

import (
	"time"
)

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
type User struct {
	UserName   string
	UserId     string
	Phone      string
	UpdateDate time.Time
	CreateDate time.Time
	Passwd     string
}

// UserRegisterRequest is used for Order.Register endpoint
// to register a user as being a schedulable entity.
type UserRegisterRequest struct {
	User *User

	// If EnforceIndex is set then the Order will only be registered if the passed
	// OrderModifyIndex matches the current Orders index. If the index is zero, the
	// register only occurs if the Order is new.
	EnforceIndex     bool
	OrderModifyIndex uint64

	WriteRequest
}

type UserResponse struct {
	Success bool
	QueryMeta
}
type UserSpecificRequest struct {
	UserID string
	QueryOptions
}
type SingleUserResponse struct {
	User *User
	QueryMeta
}
type UserDeregisterRequest struct {
	UserID string
	WriteRequest
}

// OrderListRequest is used to parameterize a list request
type UserListRequest struct {
	QueryOptions
}
type UserListResponse struct {
	Users []*User
	QueryMeta
}
