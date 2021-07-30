package v2

import (
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

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

	if leftMinute, exist := BL.blackListExist(fmt.Sprintf("%v:%v:%v", reqParam.UserGroup, reqParam.UserName, "login")); exist {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("can't login temporarily, please try again after %v minute", leftMinute)))
	}

	storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
	}

	user, exist, err := storeManager.GetUser(reqParam.UserGroup, reqParam.UserName)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	} else if !exist || reqParam.Password != user.PassWord {
		BL.setBlackList(fmt.Sprintf("%v:%v:%v", reqParam.UserGroup, reqParam.UserName, "login"), time.Minute*30)
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("user %s:%s is not exist or password is wrong", reqParam.UserGroup, reqParam.UserName)))
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

var BL *BlackList

func init() {
	BL = new(BlackList)
	BL.blackList = make(map[string]*BlackItem)
	BL.lock = new(sync.Mutex)
}

type BlackList struct {
	blackList map[string]*BlackItem
	lock      *sync.Mutex
}

type BlackItem struct {
	validateExpiredTime time.Time
	expiredTime         time.Time
	times               int
}

func (b *BlackList) setBlackList(key string, duration time.Duration) {
	b.lock.Lock()
	blackItem, ok := b.blackList[key]
	if !ok {
		blackItem = new(BlackItem)
	}
	now := time.Now()
	if now.After(blackItem.validateExpiredTime) {
		blackItem.validateExpiredTime = now.Add(time.Minute * 5)
		blackItem.times = 0
	}
	blackItem.times += 1
	if blackItem.times >= 3 {
		blackItem.expiredTime = now.Add(duration)
	}
	b.blackList[key] = blackItem
	b.lock.Unlock()
}

func (b *BlackList) blackListExist(key string) (int, bool) {
	b.lock.Lock()
	v, ok := b.blackList[key]
	b.lock.Unlock()
	now := time.Now()
	if ok && time.Now().Before(v.expiredTime) {
		return int(v.expiredTime.Sub(now).Minutes()), true
	}
	return 0, false
}

// there are at least three types of uppercase letters, lowercase characters, numbers, and special characters
func VerifyPassword(pwd string) bool {
	matchTimes := 0
	regexpSlice := []*regexp.Regexp{
		regexp.MustCompile(`[a-z]`),
		regexp.MustCompile(`[A-Z]`),
		regexp.MustCompile(`[0-9]`),
		regexp.MustCompile(`[@#$%^&*()]`),
	}
	for i := range regexpSlice {
		if regexpSlice[i].MatchString(pwd) {
			matchTimes += 1
		}
	}
	if matchTimes >= 3 && len(pwd) >= 8 {
		return true
	}
	return false
}
