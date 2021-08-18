package v2

import (
	"fmt"
	"net/http"

	"github.com/hashicorp/go-hclog"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/labstack/echo/v4"
)

// @Id RoleListV2
// @Description get role list.
// @Tags Role
// @Success 200 {object} models.RoleListResp
// @Security ApiKeyAuth
// @Param filter_tenant query string false "filter tenant"
// @Router /v2/role/list [get]
func RoleListV2(c echo.Context) error {
	logger := handler.NewLogger().Named("RoleListV2")
	reqParam := new(models.RoleListReq)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return err
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	roleList, err := storeManager.FindRoleList(reqParam.FilterTenant)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
	}

	currentUser, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	roles := make([]*common.Role, 0)
	for _, role := range roleList {
		if currentUser.Tenant == common.DefaultAdminTenant ||
			currentUser.Tenant == role.Tenant {
			roles = append(roles, role)
		}
	}

	return c.JSON(http.StatusOK, &models.RoleListResp{
		RoleList: roles,
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id CreateRoleV2
// @Description create Role.
// @Tags Role
// @Accept application/json
// @Security ApiKeyAuth
// @Param Role body models.CreateRoleReqV2 true "Role info"
// @Success 200 {object} models.CreateRoleRespV2
// @Router /v2/role/create [post]
func CreateRoleV2(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateRoleV2")
	reqParam := new(models.CreateRoleReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return err
	}

	err := UpdateRoleInfo(logger, &common.Role{
		Tenant:      reqParam.Tenant,
		Name:        reqParam.Name,
		ObjectUsers: reqParam.OperationUsers,
		ObjectType:  reqParam.OperationObjectType,
		Authority:   reqParam.Authority,
	}, true)

	return c.JSON(http.StatusOK, models.CreateRoleRespV2{BaseResp: models.BuildBaseResp(err)})
}

// @Id UpdateRoleV2
// @Description update Role info.
// @Tags Role
// @Accept application/json
// @Security ApiKeyAuth
// @Param Role body models.UpdateRoleReqV2 true "Role info"
// @Success 200 {object} models.UpdateRoleRespV2
// @Router /v2/role/update [post]
func UpdateRoleV2(c echo.Context) error {
	logger := handler.NewLogger().Named("UpdateRoleV2")
	reqParam := new(models.UpdateRoleReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return err
	}
	if reqParam.Name == common.DefaultRole {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("admin role does not support modification")))
	}
	err := UpdateRoleInfo(logger, &common.Role{
		Tenant:      reqParam.Tenant,
		Name:        reqParam.Name,
		ObjectUsers: reqParam.OperationUsers,
		ObjectType:  reqParam.OperationObjectType,
		Authority:   reqParam.Authority,
	}, false)

	return c.JSON(http.StatusOK, models.UpdateRoleRespV2{BaseResp: models.BuildBaseResp(err)})
}

func UpdateRoleInfo(logger hclog.Logger, role *common.Role, create bool) error {
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("get consul client failed: %v", err)
	}
	_, exist, err := storeManager.GetRole(role.Tenant, role.Name)
	if err != nil {
		return err
	}
	// not exist cannot update , exist cannot create
	if !exist && !create {
		return fmt.Errorf("role does not exist")
	}
	if exist && create {
		return fmt.Errorf("role already exists , cannot create")
	}

	if err := storeManager.SaveRole(role); nil != err {
		return fmt.Errorf("create metadata of role from consul failed: %v", err)
	}
	return nil
}

// @Id DeleteRoleV2
// @Description delete Role.
// @Tags Role
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param tenant formData string true "tenant"
// @Param name formData string true "role name"
// @Success 200 {object} models.DeleteRoleRespV2
// @Router /v2/role/delete [post]
func DeleteRoleV2(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteRoleV2")
	reqParam := new(models.DeleteRoleReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return err
	}
	// cannot delete default supper role
	if reqParam.Tenant == common.DefaultAdminTenant && reqParam.Name == common.DefaultAdminUser {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("cannot delete current role")))
	}
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}
	if err := storeManager.DeleteRole(reqParam.Tenant, reqParam.Name); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of Role[RoleName=%v,RoleGroup=%v] from consul failed: %v", reqParam.Name, reqParam.Tenant, err)))
	}

	return c.JSON(http.StatusOK, &models.DeleteRoleRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}
