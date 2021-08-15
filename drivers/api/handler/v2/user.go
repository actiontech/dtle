package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/dgrijalva/jwt-go"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/labstack/echo/v4"
)

// @Id UserList
// @Description get user list.
// @Tags user
// @Success 200 {object} models.UserListResp
// @Security ApiKeyAuth
// @Param filter_username query string false "filter user name"
// @Param filter_tenant query string false "filter tenant"
// @Router /v2/user/list [get]
func UserList(c echo.Context) error {
	logger := handler.NewLogger().Named("UserList")
	logger.Info("validate params")
	reqParam := new(models.UserListReq)
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
	userList, err := storeManager.FindUserList(reqParam.FilterTenant)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
	}

	currentUser, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	users := make([]*common.User, 0)
	for _, user := range userList {
		if !userHasAccess(storeManager, fmt.Sprintf("%s:%s", user.Tenant, user.Username), currentUser) {
			continue
		}
		if strings.HasPrefix(user.Username, reqParam.FilterUsername) {
			users = append(users, user)
		}
	}

	return c.JSON(http.StatusOK, &models.UserListResp{
		UserList: users,
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id TenantList
// @Description get tenant list.
// @Tags user
// @Success 200 {object} models.TenantListResp
// @Security ApiKeyAuth
// @Router /v2/tenant/list [get]
func TenantList(c echo.Context) error {
	logger := handler.NewLogger().Named("TenantList")

	currentUser, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	tenants := make([]string, 0)
	if currentUser.Tenant != common.DefaultAdminTenant {
		tenants = append(tenants, currentUser.Tenant)
	} else {
		storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
		}
		tenants, err = storeManager.FindTenantList()
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
		}
	}

	return c.JSON(http.StatusOK, &models.TenantListResp{
		TenantList: tenants,
		BaseResp:   models.BuildBaseResp(nil),
	})
}

// @Id CreateUser
// @Description create user.
// @Tags user
// @Accept application/json
// @Security ApiKeyAuth
// @Param user body models.CreateUserReqV2 true "user info"
// @Success 200 {object} models.CreateUserRespV2
// @Router /v2/user/create [post]
func CreateUser(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateUser")
	logger.Info("validate params")
	reqParam := new(models.CreateUserReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	if hasAccess, err := checkUserAccess(logger, c, fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username)); err != nil || !hasAccess {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user has no access to operate group %v ; err : %v", reqParam.Tenant, err)))
	}
	if !VerifyPassword(reqParam.PassWord) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password does not meet the rules")))
	}
	user := &common.User{
		Username:   reqParam.Username,
		Tenant:     reqParam.Tenant,
		Role:       reqParam.Role,
		CreateTime: time.Now().In(time.Local).Format(time.RFC3339),
		Password:   reqParam.PassWord,
		Remark:     reqParam.Remark,
	}
	return c.JSON(http.StatusOK, models.CreateUserRespV2{BaseResp: models.BuildBaseResp(createUser(logger, user))})
}

func createUser(logger hclog.Logger, user *common.User) error {
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return fmt.Errorf("get consul client failed: %v", err)
	}

	_, exist, err := storeManager.GetUser(user.Tenant, user.Username)
	if err != nil {
		return err
	}
	if exist {
		return fmt.Errorf("user already exists")
	}
	// create default admin role before first tenant user register
	if user.Role == common.DefaultRole {
		_, exists, err := storeManager.GetRole(user.Tenant, common.DefaultRole)
		if err != nil {
			return fmt.Errorf("create tenant admin role fail %v", err)
		}
		if !exists {
			role := common.NewDefaultRole(user.Tenant)
			if err := storeManager.SaveRole(role); nil != err {
				return fmt.Errorf("create metadata of user[userName=%v,Tenant=%v] from consul failed: %v", user.Username, user.Tenant, err)
			}
		}
	}
	if err := storeManager.SaveUser(user); nil != err {
		return fmt.Errorf("create metadata of user[userName=%v,Tenant=%v] from consul failed: %v", user.Username, user.Tenant, err)
	}
	return nil
}

// @Id UpdateUser
// @Description update user info.
// @Tags user
// @Accept application/json
// @Security ApiKeyAuth
// @Param user body models.UpdateUserReqV2 true "user info"
// @Success 200 {object} models.UpdateUserRespV2
// @Router /v2/user/update [post]
func UpdateUser(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateUser")
	logger.Info("validate params")
	reqParam := new(models.UpdateUserReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}

	if hasAccess, err := checkUserAccess(logger, c, fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username)); err != nil || !hasAccess {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user has no access to operate group %v ; err : %v", reqParam.Tenant, err)))
	}

	user, exist, err := storeManager.GetUser(reqParam.Tenant, reqParam.Username)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if !exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user %v:%v does not exist", reqParam.Tenant, reqParam.Username)))
	}

	user.Role = reqParam.Role
	user.Remark = reqParam.Remark

	if !VerifyPassword(user.Password) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password does not meet the rules")))
	}

	if err := storeManager.SaveUser(user); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of user[userName=%v,Tenant=%v] from consul failed: %v", reqParam.Username, reqParam.Tenant, err)))
	}

	return c.JSON(http.StatusOK, models.UpdateUserRespV2{BaseResp: models.BuildBaseResp(nil)})
}

// @Id ResetPassword
// @Description reset user password.
// @Tags user
// @Accept application/json
// @Security ApiKeyAuth
// @Param user body models.ResetPasswordReqV2 true "reset user password"
// @Success 200 {object} models.ResetPasswordRespV2
// @Router /v2/user/reset_password [post]
func ResetPassword(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateUser")
	logger.Info("validate params")
	reqParam := new(models.ResetPasswordReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	if hasAccess, err := checkUserAccess(logger, c, fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username)); err != nil || !hasAccess {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user has no access to operate group %v ; err : %v", reqParam.Tenant, err)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}

	user, exist, err := storeManager.GetUser(reqParam.Tenant, reqParam.Username)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if !exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user does not exist")))
	}
	if err := ValidatePassword(fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username), "reset_pwd", user.Password, reqParam.OldPassWord); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	user.Password = reqParam.PassWord
	if !VerifyPassword(user.Password) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password does not meet the rules")))
	}
	if err := storeManager.SaveUser(user); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of user[userName=%v,Tenant=%v] from consul failed: %v", reqParam.Username, reqParam.Tenant, err)))
	}

	return c.JSON(http.StatusOK, models.ResetPasswordRespV2{BaseResp: models.BuildBaseResp(nil)})
}

// @Id DeleteUser
// @Description delete user.
// @Tags user
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param tenant formData string true "tenant"
// @Param username formData string true "user name"
// @Success 200 {object} models.DeleteUserRespV2
// @Router /v2/user/delete [post]
func DeleteUser(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteUser")
	logger.Info("validate params")
	reqParam := new(models.DeleteUserReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind req param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	// cannot delete default supper user
	if reqParam.Tenant == common.DefaultAdminTenant && reqParam.Username == common.DefaultAdminUser {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("cannot delete superuser")))
	}

	if hasAccess, err := checkUserAccess(logger, c, fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username)); err != nil || !hasAccess {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user has no access to operate group %v ; err : %v", reqParam.Tenant, err)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}
	if err := storeManager.DeleteUser(reqParam.Tenant, reqParam.Username); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of user[userName=%v,userGroup=%v] from consul failed: %v", reqParam.Username, reqParam.Tenant, err)))
	}

	return c.JSON(http.StatusOK, &models.DeleteUserRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id GetCurrentUser
// @Description get current user.
// @Tags user
// @Security ApiKeyAuth
// @Success 200 {object} models.CurrentUserResp
// @Router /v2/user/current_user [get]
func GetCurrentUser(c echo.Context) error {
	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	user.Password = "*"
	return c.JSON(http.StatusOK, &models.CurrentUserResp{
		CurrentUser: user,
		BaseResp:    models.BuildBaseResp(nil),
	})
}

func getCurrentUser(c echo.Context) (*common.User, error) {
	logger := handler.NewLogger().Named("getCurrentUser")
	logger.Info("getCurrentUser")
	key := "current_user"
	currentUser := c.Get(key)
	if currentUser != nil {
		if user, ok := currentUser.(*common.User); ok {
			return user, nil
		}
	}
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return nil, err
	}
	user, exist, err := storeManager.GetUser(GetUserName(c))
	if err != nil {
		return nil, err
	} else if !exist {
		return nil, fmt.Errorf("current user is not exist")
	}

	c.Set(key, user)
	return user, nil
}

func GetUserName(c echo.Context) (string, string) {
	user := c.Get("user").(*jwt.Token)
	claims := user.Claims.(jwt.MapClaims)
	return claims["group"].(string), claims["name"].(string)
}

// @Id ListAction
// @Description list user action.
// @Tags user
// @Security ApiKeyAuth
// @Success 200 {object} models.ListActionRespV2
// @Router /v2/user/list_action [get]
func ListAction(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteUser")
	logger.Info("validate params")
	user, err := getCurrentUser(c)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}
	role, exists, err := storeManager.GetRole(user.Tenant, user.Role)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get role failed: %v", handler.ConsulAddr, err)))
	}
	if !exists {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user does not belong to any role")))
	}
	authority := make(map[string][]*models.ActionItem, 0)
	err = json.Unmarshal([]byte(role.Authority), &authority)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.ListActionRespV2{
		Authority: authority,
		BaseResp:  models.BuildBaseResp(nil),
	})
}

func checkUserAccess(logger hclog.Logger, c echo.Context, operationUser string) (bool, error) {
	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return false, fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}
	currentUser, err := getCurrentUser(c)
	if err != nil {
		return false, err
	}
	access := userHasAccess(storeManager, operationUser, currentUser)
	return access, nil
}

func userHasAccess(storeManager *common.StoreManager, operationUser string, currentUser *common.User) bool {
	// get current role
	role, exists, err := storeManager.GetRole(currentUser.Tenant, currentUser.Role)
	if err != nil || !exists {
		return false
	}
	// currentUser name and tenant
	// operationUser  =  tenant:username
	operationUserInfo := strings.Split(operationUser, ":")
	if len(operationUserInfo) < 2 {
		return false
	}
	tenant, username := operationUserInfo[0], operationUserInfo[1]

	// platform can operation all user data
	// tenant only operation current tenant user
	if currentUser.Tenant != common.DefaultAdminTenant && currentUser.Tenant != tenant {
		return false
	}
	if (role.ObjectType == "all") || (currentUser.Tenant == tenant && currentUser.Username == username) {
		return true
	}
	// operation user is managed by role(object users: tenant:user)
	for _, enableUser := range role.ObjectUsers {
		if operationUser == enableUser {
			return true
		}
	}
	return false

}
