package v2

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/g"
)

var BL *Blacklist

func init() {
	BL = new(Blacklist)
	BL.blacklist = make(map[string]*BlacklistItem)
	BL.lock = new(sync.Mutex)
}

type Blacklist struct {
	blacklist map[string]*BlacklistItem
	lock      *sync.Mutex
}

type BlacklistItem struct {
	validateExpiredTime time.Time
	expiredTime         time.Time
	times               int
}

func (b *Blacklist) setBlacklist(key string, duration time.Duration) {
	b.lock.Lock()
	blackItem, ok := b.blacklist[key]
	if !ok {
		blackItem = new(BlacklistItem)
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
	b.blacklist[key] = blackItem
	b.lock.Unlock()
}

func (b *Blacklist) blacklistExist(key string) (int, bool) {
	b.lock.Lock()
	v, ok := b.blacklist[key]
	b.lock.Unlock()
	now := time.Now()
	if ok && time.Now().Before(v.expiredTime) {
		return int(v.expiredTime.Sub(now).Minutes()), true
	}
	return 0, false
}

// validate current user in blacklist and update blacklist
func ValidatePassword(blacklistKey, currentPwd, verifiedPwd string) error {
	realCurrentPwd, err := handler.DecryptPasswordSupportNoRsaKey(currentPwd, g.RsaPrivateKey)
	if err != nil {
		return fmt.Errorf("decrypt current password err")
	}
	realVerifiedPwd, err := handler.DecryptPasswordSupportNoRsaKey(verifiedPwd, g.RsaPrivateKey)
	if err != nil {
		return fmt.Errorf("decrypt verified password err")
	}
	if realCurrentPwd != realVerifiedPwd {
		BL.setBlacklist(blacklistKey, time.Minute*30)
		return fmt.Errorf("user or password is wrong")
	}
	return nil
}

// there are at least three types of uppercase letters, lowercase characters, numbers, and special characters
func VerifyPassword(encryptPwd string) bool {
	realPassword, err := handler.DecryptPasswordSupportNoRsaKey(encryptPwd, g.RsaPrivateKey)
	if err != nil {
		return false
	}
	matchTimes := 0
	regexpSlice := []*regexp.Regexp{
		regexp.MustCompile(`[a-z]`),
		regexp.MustCompile(`[A-Z]`),
		regexp.MustCompile(`[0-9]`),
		regexp.MustCompile(`[@#$%^&*()]`),
	}
	for i := range regexpSlice {
		if regexpSlice[i].MatchString(realPassword) {
			matchTimes += 1
		}
	}
	if matchTimes >= 3 && len(realPassword) >= 8 {
		return true
	}
	return false
}
