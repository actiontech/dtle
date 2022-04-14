package models

import "github.com/actiontech/dtle/driver/common"

type RoleListReq struct {
	FilterTenant string `query:"filter_tenant"`
}

type RoleListResp struct {
	RoleList []*common.Role `json:"role_list"`
	BaseResp
}

type CreateRoleReqV2 struct {
	Tenant              string   `json:"tenant"`
	Name                string   `json:"name"`
	OperationUsers      []string `json:"operation_users"`
	Authority           string   `json:"authority"`
	OperationObjectType string   `json:"operation_object_type"`
}

type CreateRoleRespV2 struct {
	BaseResp
}

type UpdateRoleReqV2 struct {
	Tenant              string   `json:"tenant"`
	Name                string   `json:"name"`
	OperationUsers      []string `json:"operation_users"`
	Authority           string   `json:"authority"`
	OperationObjectType string   `json:"operation_object_type"`
}

type UpdateRoleRespV2 struct {
	BaseResp
}

type DeleteRoleReqV2 struct {
	Name   string `form:"name" validate:"required"`
	Tenant string `form:"tenant" validate:"required"`
}

type DeleteRoleRespV2 struct {
	BaseResp
}
type MenuItem struct {
	Name       string       `json:"name"`
	TextCn     string       `json:"text_cn"`
	TextEn     string       `json:"text_en"`
	MenuLevel  int          `json:"menu_level"`
	MenuUrl    string       `json:"menu_url"`
	Id         int          `json:"id"`
	ParentId   int          `json:"parent_id"`
	Operations []ButtonItem `json:"operations"`
	AdminOnly  bool         `json:"admin_only"`
}

type ButtonItem struct {
	Action string `json:"action"`
	Uri    string `json:"uri"`
	TextCn string `json:"text_cn"`
	TextEn string `json:"text_en"`
}
