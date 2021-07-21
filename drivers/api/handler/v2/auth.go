package v2

import (
	"fmt"
	"net/http"
	"time"

	"github.com/docker/libkv/store"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo/v4"
)

// @Summary 用户登录
// @Description user login
// @Tags user
// @Id loginV2
// @Param user body models.UserLoginReqV2 true "user login request"
// @Success 200 {object} models.GetUserLoginResV2
// @router /v2/login [post]
func Login(c echo.Context) error {
	logger := handler.NewLogger().Named("UserList")
	logger.Info("validate params")

	reqParam := new(models.UserLoginReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind reqParam param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}

	user, err := storeManager.GetUser(reqParam.UserGroup, reqParam.UserName)
	if err == store.ErrKeyNotFound {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user %s:%s is not exist", reqParam.UserGroup, reqParam.UserName)))
	} else if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))

	}
	if reqParam.Password != user.PassWord {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("password is wrong")))
	}
	// Create token
	token := jwt.New(jwt.SigningMethodHS256)
	claims := token.Claims.(jwt.MapClaims)
	claims["group"] = reqParam.UserGroup
	claims["name"] = reqParam.UserName
	claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

	t, err := token.SignedString([]byte(common.JWTSecret))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err)
	}
	return c.JSON(http.StatusOK, &models.GetUserLoginResV2{
		Data:     models.UserLoginResV2{Token: t},
		BaseResp: models.BuildBaseResp(nil),
	})
}
