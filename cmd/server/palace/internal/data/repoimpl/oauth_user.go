package repoimpl

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/aide-family/moon/api/merr"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz/bo/auth"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz/repository"
	"github.com/aide-family/moon/cmd/server/palace/internal/data"
	"github.com/aide-family/moon/cmd/server/palace/internal/palaceconf"
	"github.com/aide-family/moon/pkg/palace/model"
	"github.com/aide-family/moon/pkg/palace/model/query"
	"github.com/aide-family/moon/pkg/util/cipher"
	"github.com/aide-family/moon/pkg/util/format"
	"github.com/aide-family/moon/pkg/util/types"
	"github.com/aide-family/moon/pkg/vobj"
	"gorm.io/gorm/clause"

	"github.com/go-kratos/kratos/v2/errors"
	"gorm.io/gorm"
)

func NewGithubUserRepository(bc *palaceconf.Bootstrap, data *data.Data) repository.OAuth {
	return &githubUserRepositoryImpl{data: data, bc: bc}
}

type githubUserRepositoryImpl struct {
	data *data.Data
	bc   *palaceconf.Bootstrap
}

func buildSysUserModel(u auth.IOAuthUser, pass types.Password) *model.SysUser {
	return &model.SysUser{
		Username: u.GetUsername(),
		Nickname: u.GetNickname(),
		Password: pass.String(),
		Email:    u.GetEmail(),
		Remark:   u.GetRemark(),
		Avatar:   u.GetAvatar(),
		Salt:     pass.GetSalt(),
		Gender:   vobj.GenderUnknown,
		Role:     vobj.RoleUser,
		Status:   vobj.StatusEnable,
	}
}

func genPassword() (string, types.Password) {
	randPass := cipher.MD5(time.Now().String())[:8]
	password := types.NewPassword(cipher.MD5(randPass + "3c4d9a0a5a703938dd1d2d46e1c924f9"))
	return randPass, password
}

func (g *githubUserRepositoryImpl) GetSysUserByOAuthID(ctx context.Context, u uint32, app vobj.OAuthAPP) (*model.SysOAuthUser, error) {
	userQuery := query.Use(g.data.GetMainDB(ctx))
	oauthUser, err := userQuery.SysOAuthUser.WithContext(ctx).Where(
		userQuery.SysOAuthUser.OAuthID.Eq(u),
		userQuery.SysOAuthUser.APP.Eq(app.GetValue()),
	).First()
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nToastUserNotFound(ctx)
	}
	return oauthUser, nil
}

func (g *githubUserRepositoryImpl) SetEmail(ctx context.Context, u uint32, s string) (sysUser *model.SysUser, err error) {
	userQuery := query.Use(g.data.GetMainDB(ctx))
	oauthUser, err := userQuery.SysOAuthUser.WithContext(ctx).Where(userQuery.SysOAuthUser.ID.Eq(u)).First()
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nToastUserNotFound(ctx)
	}

	// 查询此邮箱有没有被绑定， 如果被绑定， 则直接关联该平台
	if sysUser, err = g.getSysUserByEmail(ctx, s); types.IsNil(err) {
		if _, err = userQuery.SysOAuthUser.WithContext(ctx).Where(userQuery.SysOAuthUser.ID.Eq(u)).UpdateSimple(userQuery.SysOAuthUser.SysUserID.Value(sysUser.ID)); err != nil {
			return nil, err
		}
		return sysUser, nil
	}

	if !types.IsNil(err) && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, merr.ErrorI18nToastUserNotFound(ctx)
	}

	iuser, err := auth.NewOAuthRowData(oauthUser.APP, oauthUser.Row)
	if !types.IsNil(err) {
		return nil, err
	}
	randPass, password := genPassword()
	sysUser = buildSysUserModel(iuser, password)

	err = userQuery.Transaction(func(tx *query.Query) error {
		if err = tx.SysUser.WithContext(ctx).Create(sysUser); err != nil {
			return err
		}
		_, err = tx.SysOAuthUser.WithContext(ctx).Where(userQuery.SysOAuthUser.ID.Eq(oauthUser.ID)).
			UpdateSimple(userQuery.SysOAuthUser.SysUserID.Value(sysUser.ID))
		return err
	})
	if !types.IsNil(err) {
		return nil, err
	}

	_ = g.sendUserPassword(ctx, sysUser, randPass)

	return sysUser, nil
}

func (g *githubUserRepositoryImpl) OAuthUserFirstOrCreate(ctx context.Context, user auth.IOAuthUser) (sysUser *model.SysUser, err error) {
	userQuery := query.Use(g.data.GetMainDB(ctx)).SysOAuthUser
	first, err := userQuery.WithContext(ctx).Where(userQuery.OAuthID.Eq(user.GetOAuthID()), userQuery.APP.Eq(user.GetAPP().GetValue())).First()
	if types.IsNil(err) {
		return g.getSysUserByID(ctx, first.SysUserID)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}

	// 根据邮箱查询系统用户
	sysUser, err = g.getSysUserByEmail(ctx, user.GetEmail())
	if !types.IsNil(err) {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		randPass, password := genPassword()
		sysUser = buildSysUserModel(user, password)
		defer func() {
			if err == nil {
				// 发送用户密码到用户邮箱
				g.sendUserPassword(ctx, sysUser, randPass)
			}
		}()
	}
	// 调试用
	//sysUser.Email = "1058165620@qq.com"
	err = query.Use(g.data.GetMainDB(ctx)).Transaction(func(tx *query.Query) error {
		if err := types.CheckEmail(user.GetEmail()); types.IsNil(err) {
			// 创建系统用户
			if err = tx.SysUser.Clauses(clause.OnConflict{DoNothing: true}).Create(sysUser); !types.IsNil(err) {
				return err
			}
		}

		sysOAuthUser := &model.SysOAuthUser{
			OAuthID:   user.GetOAuthID(),
			SysUserID: sysUser.ID,
			Row:       user.String(),
			APP:       user.GetAPP(),
		}
		if err = tx.SysOAuthUser.WithContext(ctx).Create(sysOAuthUser); !types.IsNil(err) {
			return err
		}
		return nil
	})
	if !types.IsNil(err) {
		return nil, err
	}
	return sysUser, nil
}

func (g *githubUserRepositoryImpl) getSysUserByID(ctx context.Context, id uint32) (*model.SysUser, error) {
	userQuery := query.Use(g.data.GetMainDB(ctx)).SysUser
	return userQuery.WithContext(ctx).Where(userQuery.ID.Eq(id)).First()
}

func (g *githubUserRepositoryImpl) getSysUserByEmail(ctx context.Context, email string) (*model.SysUser, error) {
	userQuery := query.Use(g.data.GetMainDB(ctx)).SysUser
	return userQuery.WithContext(ctx).Where(userQuery.Email.Eq(email)).First()
}

//go:embed welcome.html
var body string

func (g *githubUserRepositoryImpl) sendUserPassword(_ context.Context, user *model.SysUser, pass string) error {
	if err := types.CheckEmail(user.Email); err != nil {
		return err
	}

	body = format.Formatter(body, map[string]string{
		"Username":    user.Email,
		"Password":    pass,
		"RedirectURI": g.bc.GetRedirectUri(),
		"APP":         g.bc.GetServer().GetName(),
		"Remark":      g.bc.GetServer().GetMetadata()["description"],
	})
	// 发送用户密码到用户邮箱
	return g.data.GetEmailer().SetSubject("欢迎使用"+g.bc.GetServer().GetName()).SetTo(user.Email).SetBody(body, "text/html").Send()
}

//go:embed verify_email.html
var verifyEmailHtml string

// SendVerifyEmail 发送验证邮件
func (g *githubUserRepositoryImpl) SendVerifyEmail(ctx context.Context, email string) error {
	if err := types.CheckEmail(email); err != nil {
		return err
	}
	// 生成验证码
	code := strings.ToUpper(cipher.MD5(time.Now().String())[:6])
	// 缓存验证码
	if err := g.data.GetCacher().Set(ctx, fmt.Sprintf("email_verify_code:%s", email), code, 5*time.Minute); err != nil {
		return err
	}
	// 发送验证码到用户邮箱
	emailBody := format.Formatter(verifyEmailHtml, map[string]string{
		"Email":       email,
		"Code":        code,
		"RedirectURI": g.bc.GetRedirectUri(),
		"APP":         g.bc.GetServer().GetName(),
		"Remark":      g.bc.GetServer().GetMetadata()["description"],
	})
	return g.data.GetEmailer().SetSubject("欢迎使用"+g.bc.GetServer().GetName()).SetTo(email).SetBody(emailBody, "text/html").Send()
}

// CheckVerifyEmailCode 检查验证码
func (g *githubUserRepositoryImpl) CheckVerifyEmailCode(ctx context.Context, email, code string) error {
	if err := types.CheckEmail(email); err != nil {
		return err
	}
	// 验证码是否正确
	if v, err := g.data.GetCacher().Get(ctx, fmt.Sprintf("email_verify_code:%s", email)); err != nil || v != code {
		return merr.ErrorI18nAlertEmailCaptchaErr(ctx)
	}
	return nil
}
