package models

import "github.com/actiontech/dtle/drivers/mysql/common"

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

type ActionItem struct {
	Action string `json:"action"`
	Uri    string `json:"uri"`
}
