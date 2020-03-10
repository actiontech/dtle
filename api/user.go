/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */
package api

import "time"

type User struct {
	UserName   string
	Passwd     string
	VarifyCode string
	VerifyId   string
}
type ListUser struct {
	UserName   string
	UserId     string
	UpdateDate string
	Phone      string
	CreateDate string
}

type EditUser struct {
	UserName   string
	ID         string
	UserID         string
	Phone      string
	Passwd     string
	CreateDate time.Time
}

type AddUser struct {
	UserName string
	Passwd   string
	Phone    string
}
