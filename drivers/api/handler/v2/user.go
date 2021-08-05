package v2

import (
	"fmt"
	"net/http"
	"strings"
	"time"

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
		if currentUser.Tenant != common.DefaultAdminGroup &&
			currentUser.Tenant != user.Tenant {
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

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("get consul client failed: %v", err)))
	}

	if hasAccess, err := checkUserAccess(c, reqParam.Tenant); err != nil || !hasAccess {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("current user has no access to operate group %v ; err : %v", reqParam.Tenant, err)))
	}

	user, exist, err := storeManager.GetUser(reqParam.Tenant, reqParam.Username)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	if exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user already exists")))
	}
	if !VerifyPassword(reqParam.PassWord) {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password does not meet the rules")))
	}
	user = &common.User{
		Username:   reqParam.Username,
		Tenant:     reqParam.Tenant,
		Role:       reqParam.Role,
		CreateTime: time.Now().In(time.Local).Format(time.RFC3339),
		Password:   reqParam.PassWord,
		Remark:     reqParam.Remark,
	}
	if err := storeManager.SaveUser(user); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("create metadata of user[userName=%v,Tenant=%v] from consul failed: %v", reqParam.Username, reqParam.Tenant, err)))
	}

	return c.JSON(http.StatusOK, models.CreateUserRespV2{BaseResp: models.BuildBaseResp(nil)})
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

	if hasAccess, err := checkUserAccess(c, reqParam.Tenant); err != nil || !hasAccess {
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

	if hasAccess, err := checkUserAccess(c, reqParam.Tenant); err != nil || !hasAccess {
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

	if err := ValidateBlackList(fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username), "reset_pwd", user.Password, reqParam.OldPassWord); err != nil {
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
	if hasAccess, err := checkUserAccess(c, reqParam.Tenant); err != nil || !hasAccess {
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

func checkUserAccess(c echo.Context, group string) (bool, error) {
	currentUser, err := getCurrentUser(c)
	if err != nil {
		return false, err
	}
	if currentUser.Tenant != common.DefaultAdminGroup && currentUser.Tenant != group {
		return false, nil
	}
	return true, nil
}

// name value like dba:dba1(tenant:username)
func GetUserTenant(name string) (string, string) {
	nameSlice := strings.Split(name, ":")
	if len(nameSlice) != 2 {
		return "", ""
	}
	return nameSlice[0], nameSlice[1]
}
