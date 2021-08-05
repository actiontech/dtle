package v2

import (
	"fmt"
	"net/http"
	"time"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo/v4"
)

// @Summary user login
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

	user, exist, err := storeManager.GetUser(reqParam.Tenant, reqParam.Username)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	} else if !exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user or password is wrong")))
	}

	if err := ValidateBlackList(fmt.Sprintf("%s:%s", reqParam.Tenant, reqParam.Username), "login", reqParam.Password, user.Password); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	// Create token
	token := jwt.New(jwt.SigningMethodHS256)
	claims := token.Claims.(jwt.MapClaims)
	claims["group"] = reqParam.Tenant
	claims["name"] = reqParam.Username
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
