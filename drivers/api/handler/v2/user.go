package v2

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/docker/libkv/store"
	"github.com/labstack/echo/v4"
)

// @Id UserList
// @Description get user list.
// @Tags user
// @Success 200 {object} models.UserListResp
// @Param filter_user_name query string false "filter user name"
// @Param filter_user_group query string false "filter user group"
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
	userList, err := storeManager.FindUserList(reqParam.FilterUserGroup)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v ; get job status list failed: %v", handler.ConsulAddr, err)))
	}

	i := 0
	for _, user := range userList {
		if strings.HasPrefix(user.UserName, reqParam.FilterUserName) {
			userList[i] = user
			i++
		}
	}
	userList = userList[:i]

	return c.JSON(http.StatusOK, &models.UserListResp{
		UserList: userList,
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id CreateOrUpdateUser
// @Description create or update user.
// @Tags user
// @Accept application/json
// @Param user body models.CreateOrUpdateUserReq true "user info"
// @Success 200 {object} models.CreateOrUpdateUserResp
// @Router /v2/user/update [post]
func CreateOrUpdateUser(c echo.Context) error {
	logger := handler.NewLogger().Named("CreateOrUpdateUser")
	logger.Info("validate params")
	reqParam := new(models.CreateOrUpdateUserReq)
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

	user, err := storeManager.GetUser(reqParam.UserGroup, reqParam.UserName)
	if err == nil {
		// update
		if reqParam.PassWord != "" {
			user.PassWord = reqParam.PassWord
		}
		user.Role = reqParam.Role
		user.ContactInfo = reqParam.ContactInfo
		user.Principal = reqParam.Principal
	} else if store.ErrKeyNotFound == err {
		// create
		if reqParam.PassWord == "" {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password is forbidden to be empty")))
		}
		user = &models.User{
			UserName:    reqParam.UserName,
			UserGroup:   reqParam.UserGroup,
			Role:        reqParam.Role,
			CreateTime:  time.Now().In(time.Local).Format(time.RFC3339),
			ContactInfo: reqParam.ContactInfo,
			Principal:   reqParam.Principal,
			PassWord:    reqParam.PassWord,
		}
	} else {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	if err := storeManager.SaveUser(user); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of user[userName=%v,userGroup=%v] from consul failed: %v", reqParam.UserName, reqParam.UserGroup, err)))
	}

	return c.JSON(http.StatusOK, models.CreateOrUpdateUserResp{BaseResp: models.BuildBaseResp(nil)})
}

// @Id DeleteUser
// @Description delete user.
// @Tags user
// @accept application/x-www-form-urlencoded
// @Param user_group formData string true "user group name"
// @Param user_name formData string true "user name"
// @Success 200 {object} models.DeleteUserResp
// @Router /v2/user/delete [post]
func DeleteUser(c echo.Context) error {
	logger := handler.NewLogger().Named("DeleteUser")
	logger.Info("validate params")
	reqParam := new(models.DeleteUserReq)
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
	if err := storeManager.DeleteUser(reqParam.UserGroup, reqParam.UserName); nil != err {
		return c.JSON(http.StatusInternalServerError,
			models.BuildBaseResp(fmt.Errorf("delete metadata of user[userName=%v,userGroup=%v] from consul failed: %v", reqParam.UserName, reqParam.UserGroup, err)))
	}

	return c.JSON(http.StatusOK, &models.DeleteUserResp{
		BaseResp: models.BuildBaseResp(nil),
	})
}
