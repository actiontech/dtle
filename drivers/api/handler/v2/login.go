package v2

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/actiontech/dtle/g"

	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/common"
	"github.com/dgrijalva/jwt-go"
	"github.com/labstack/echo/v4"
	"github.com/mojocn/base64Captcha"
)

var once sync.Once

// @Summary user loginV2
// @Description user login
// @Tags user
// @Id loginV2
// @Param user body models.UserLoginReqV2 true "user login request"
// @Success 200 {object} models.GetUserLoginResV2
// @router /v2/login [post]
func LoginV2(c echo.Context) error {
	logger := handler.NewLogger().Named("LoginV2")

	reqParam := new(models.UserLoginReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	blacklistKey := fmt.Sprintf("%s:%s:%s", reqParam.Tenant, reqParam.Username, "login")
	if leftMinute, exist := BL.blacklistExist(blacklistKey); exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("you cannot be login temporarily, please try again after %v minute", leftMinute)))
	}
	if !store.Verify(reqParam.CaptchaId, reqParam.Captcha, true) {
		BL.setBlacklist(blacklistKey, time.Minute*30)
		return c.JSON(http.StatusBadRequest, models.BuildBaseResp(fmt.Errorf("verfied failed")))
	}
	token, err := innerLogin(logger, reqParam.Tenant, reqParam.Username, reqParam.Password, blacklistKey)
	if err != nil {
		return c.JSON(http.StatusBadRequest, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.GetUserLoginResV2{
		Data:     models.UserLoginResV2{Token: token},
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Summary user LoginWithoutVerifyCodeV2
// @Description user login Without Verify Code
// @Tags user
// @Id LoginWithoutVerifyCodeV2
// @Param user body models.LoginWithoutVerifyCodeReqV2 true "user login request"
// @Success 200 {object} models.GetUserLoginResV2
// @router /v2/loginWithoutVerifyCode [post]
func LoginWithoutVerifyCodeV2(c echo.Context) error {
	logger := handler.NewLogger().Named("LoginWithoutVerifyCodeV2")

	reqParam := new(models.LoginWithoutVerifyCodeReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	blacklistKey := fmt.Sprintf("%s:%s:%s", reqParam.Tenant, reqParam.Username, "login")
	if leftMinute, exist := BL.blacklistExist(blacklistKey); exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("you cannot be login temporarily, please try again after %v minute", leftMinute)))
	}
	token, err := innerLogin(logger, reqParam.Tenant, reqParam.Username, reqParam.Password, blacklistKey)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.GetUserLoginResV2{
		Data:     models.UserLoginResV2{Token: token},
		BaseResp: models.BuildBaseResp(nil),
	})
}

func innerLogin(logger g.LoggerType, tenant, username, password, blacklistKey string) (string, error) {
	once.Do(createPlatformUser)

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return "", fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)
	}

	user, exist, err := storeManager.GetUser(tenant, username)
	if err != nil {
		return "", err
	} else if !exist {
		BL.setBlacklist(blacklistKey, time.Minute*30)
		return "", fmt.Errorf("user or password is wrong")
	}

	if err := ValidatePassword(blacklistKey, password, user.Password); err != nil {
		return "", err
	}

	// Create token
	token := jwt.New(jwt.SigningMethodHS256)
	claims := token.Claims.(jwt.MapClaims)
	claims["group"] = tenant
	claims["name"] = username
	claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

	t, err := token.SignedString([]byte(common.JWTSecret))
	if err != nil {
		return "", err
	}
	return t, nil
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
	logger := handler.NewLogger().Named("CaptchaV2")
	reqParam := new(models.VerifyCodeReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
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
	return c.JSON(http.StatusOK, models.CaptchaRespV2{Id: id, DataScheme: b64s, BaseResp: models.BuildBaseResp(nil)})
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
	if g.RsaPrivateKey != "" {
		user.Password = common.DefaultEncryptAdminPwd
	}
	_ = createUser(logger, user)
}
