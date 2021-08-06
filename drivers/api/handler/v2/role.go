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

// @Id RoleList
// @Description get role list.
// @Tags Role
// @Success 200 {object} models.RoleListResp
// @Security ApiKeyAuth
// @Param filter_tenant query string false "filter tenant"
// @Router /v2/role/list [get]
func RoleList(c echo.Context) error {
	logger := handler.NewLogger().Named("RoleList")
	logger.Info("validate params")
	reqParam := new(models.RoleListReq)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
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

// @Id CreateRole
// @Description create Role.
// @Tags Role
// @Accept application/json
// @Security ApiKeyAuth
// @Param Role body models.CreateRoleReqV2 true "Role info"
// @Success 200 {object} models.CreateRoleRespV2
// @Router /v2/role/create [post]
func CreateRole(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateRole")
	logger.Info("validate params")
	reqParam := new(models.CreateRoleReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
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

// @Id UpdateRole
// @Description update Role info.
// @Tags Role
// @Accept application/json
// @Security ApiKeyAuth
// @Param Role body models.UpdateRoleReqV2 true "Role info"
// @Success 200 {object} models.UpdateRoleRespV2
// @Router /v2/role/update [post]
func UpdateRole(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateRole")
	logger.Info("validate params")
	reqParam := new(models.UpdateRoleReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
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

// @Id DeleteRole
// @Description delete Role.
// @Tags Role
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param tenant formData string true "tenant"
// @Param name formData string true "role name"
// @Success 200 {object} models.DeleteRoleRespV2
// @Router /v2/role/delete [post]
func DeleteRole(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteRole")
	logger.Info("validate params")
	reqParam := new(models.DeleteRoleReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
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
