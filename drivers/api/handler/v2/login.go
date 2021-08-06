package v2

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo/v4"
	"github.com/mojocn/base64Captcha"
)

var once sync.Once

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
	once.Do(createPlatformUser)
	reqParam := new(models.UserLoginReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind reqParam param failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	if !store.Verify(reqParam.CaptchaId, reqParam.Captcha, true) {
		return c.JSON(http.StatusBadRequest, models.BuildBaseResp(fmt.Errorf("verfied failed")))
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

var store = base64Captcha.DefaultMemStore

// @Summary create base64Captcha
// @Description create base64Captcha
// @Tags user
// @Id CaptchaV2
// @Param captcha_type formData string true "captcha type" Enums(default,audio)
// @Success 200 {object} models.CaptchaRespV2
// @router /v2/login/captcha [post]
func CaptchaV2(c echo.Context) error {
	//parse request parameters
	reqParam := new(models.VerifyCodeReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("bind reqParam reqParam failed, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	//create base64 encoding captcha
	var driver base64Captcha.Driver

	switch reqParam.CaptchaType {
	case "audio":
		driver = base64Captcha.DefaultDriverAudio
	default:
		driver = base64Captcha.DefaultDriverDigit
	}
	captcha := base64Captcha.NewCaptcha(driver, store)
	id, b64s, err := captcha.Generate()
	if err != nil {
		return c.JSON(http.StatusOK, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, models.CaptchaRespV2{Id: id, DataScheme: b64s})
}

func createPlatformUser() {
	logger := handler.NewLogger().Named("CreatePlatformUser")
	user := &common.User{
		Username:   common.DefaultAdminUser,
		Tenant:     common.DefaultAdminTenant,
		Role:       common.DefaultRole,
		Password:   common.DefaultAdminPwd,
		CreateTime: time.Now().In(time.Local).Format(time.RFC3339),
	}
	_ = createUser(logger, user)
}
