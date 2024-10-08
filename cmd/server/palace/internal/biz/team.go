package biz

import (
	"context"

	"github.com/aide-family/moon/api/merr"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz/bo"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz/repository"
	"github.com/aide-family/moon/pkg/helper/middleware"
	"github.com/aide-family/moon/pkg/palace/model"
	"github.com/aide-family/moon/pkg/palace/model/bizmodel"
	"github.com/aide-family/moon/pkg/util/types"
	"github.com/aide-family/moon/pkg/vobj"
	"github.com/go-kratos/kratos/v2/errors"
	"gorm.io/gorm"
)

// NewTeamBiz 创建团队业务
func NewTeamBiz(teamRepo repository.Team) *TeamBiz {
	return &TeamBiz{
		teamRepo: teamRepo,
	}
}

// TeamBiz 团队业务
type TeamBiz struct {
	teamRepo repository.Team
}

// CreateTeam 创建团队
func (t *TeamBiz) CreateTeam(ctx context.Context, params *bo.CreateTeamParams) (*model.SysTeam, error) {
	if types.IsNil(params) {
		return nil, merr.ErrorNotification("参数为空")
	}
	teamDo, err := t.teamRepo.CreateTeam(ctx, params)
	if !types.IsNil(err) {
		if merr.IsAlertTeamNameExistErr(err) {
			return nil, err
		}
		return nil, merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return teamDo, nil
}

// UpdateTeam 更新团队
func (t *TeamBiz) UpdateTeam(ctx context.Context, team *bo.UpdateTeamParams) error {
	// 不是管理员不允许修改
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}

	return t.teamRepo.UpdateTeam(ctx, team)
}

// GetTeam 获取团队信息
func (t *TeamBiz) GetTeam(ctx context.Context, teamID uint32) (*model.SysTeam, error) {
	if teamID == 0 {
		return nil, merr.ErrorI18nToastTeamNotFound(ctx)
	}
	teamList, err := t.ListTeam(ctx, &bo.QueryTeamListParams{IDs: []uint32{teamID}})
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	if len(teamList) == 0 {
		return nil, merr.ErrorI18nToastTeamNotFound(ctx)
	}
	return teamList[0], nil
}

// ListTeam 获取团队列表
func (t *TeamBiz) ListTeam(ctx context.Context, params *bo.QueryTeamListParams) ([]*model.SysTeam, error) {
	claims, ok := middleware.ParseJwtClaims(ctx)
	if !ok {
		return nil, merr.ErrorI18nUnauthorized(ctx)
	}
	// 不是管理员不允许修改
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		params.UserID = claims.GetUser()
	}
	list, err := t.teamRepo.GetTeamList(ctx, params)
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return list, nil
}

// UpdateTeamStatus 更新团队状态
func (t *TeamBiz) UpdateTeamStatus(ctx context.Context, status vobj.Status, ids ...uint32) error {
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	if err := t.teamRepo.UpdateTeamStatus(ctx, status, ids...); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// GetUserTeamList 获取用户团队列表
func (t *TeamBiz) GetUserTeamList(ctx context.Context, userID uint32) ([]*model.SysTeam, error) {
	list, err := t.teamRepo.GetUserTeamList(ctx, userID)
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return list, nil
}

// AddTeamMember 添加团队成员
func (t *TeamBiz) AddTeamMember(ctx context.Context, params *bo.AddTeamMemberParams) error {
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	if err := t.teamRepo.AddTeamMember(ctx, params); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// RemoveTeamMember 移除团队成员
func (t *TeamBiz) RemoveTeamMember(ctx context.Context, params *bo.RemoveTeamMemberParams) error {
	if len(params.MemberIds) == 0 {
		return nil
	}

	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	// 查询团队管理员
	teamMemberList, err := t.teamRepo.ListTeamMember(ctx, &bo.ListTeamMemberParams{
		ID:        params.ID,
		MemberIDs: params.MemberIds,
	})
	if !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	if len(teamMemberList) == 0 {
		return nil
	}
	// 查询团队信息
	teamInfo, err := t.teamRepo.GetTeamDetail(ctx, params.ID)
	if !types.IsNil(err) {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return merr.ErrorI18nToastTeamNotFound(ctx)
		}
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}

	claims, ok := middleware.ParseJwtClaims(ctx)
	if !ok {
		return merr.ErrorI18nUnauthorized(ctx)
	}
	for _, teamMember := range teamMemberList {
		role := teamMember.Role
		if role.IsSuperadmin() || role.IsAdmin() || teamMember.UserID == teamInfo.LeaderID {
			return merr.ErrorI18nToastUserNotAllowRemoveAdmin(ctx)
		}
		if teamMember.UserID == claims.GetUser() {
			return merr.ErrorI18nToastUserNotAllowRemoveSelf(ctx)
		}
	}

	// 判断移除的人员中是否包含当前用户和管理员
	if err = t.teamRepo.RemoveTeamMember(ctx, params); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// SetTeamAdmin 设置团队管理员
func (t *TeamBiz) SetTeamAdmin(ctx context.Context, params *bo.SetMemberAdminParams) error {
	claims, ok := middleware.ParseJwtClaims(ctx)
	if !ok {
		return merr.ErrorI18nUnauthorized(ctx)
	}
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	// 不能设置自己
	for _, memberID := range params.MemberIDs {
		if memberID == claims.GetUser() {
			return merr.ErrorI18nToastUserNotAllowOperateAdmin(ctx)
		}
	}
	if err := t.teamRepo.SetMemberAdmin(ctx, params); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// SetMemberRole 设置团队成员角色
func (t *TeamBiz) SetMemberRole(ctx context.Context, params *bo.SetMemberRoleParams) error {
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	if err := t.teamRepo.SetMemberRole(ctx, params); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// ListTeamMember 获取团队成员列表
func (t *TeamBiz) ListTeamMember(ctx context.Context, params *bo.ListTeamMemberParams) ([]*bizmodel.SysTeamMember, error) {
	list, err := t.teamRepo.ListTeamMember(ctx, params)
	if !types.IsNil(err) {
		return nil, merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return list, nil
}

// TransferTeamLeader 移交团队领导
func (t *TeamBiz) TransferTeamLeader(ctx context.Context, params *bo.TransferTeamLeaderParams) error {
	// 获取团队信息
	team, err := t.teamRepo.GetTeamDetail(ctx, params.ID)
	if !types.IsNil(err) {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return merr.ErrorI18nToastTeamNotFound(ctx)
		}
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	if team.LeaderID != params.OldLeaderID {
		return merr.ErrorI18nForbidden(ctx)
	}
	if team.LeaderID == params.LeaderID {
		return nil
	}
	if err = t.teamRepo.TransferTeamLeader(ctx, params); !types.IsNil(err) {
		return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
	}
	return nil
}

// SetTeamMailConfig 设置团队邮件配置
func (t *TeamBiz) SetTeamMailConfig(ctx context.Context, params *bo.SetTeamMailConfigParams) error {
	if !middleware.GetTeamRole(ctx).IsAdminOrSuperAdmin() && !middleware.GetUserRole(ctx).IsAdminOrSuperAdmin() {
		return merr.ErrorI18nForbidden(ctx)
	}
	// 查询团队邮件配置
	_, err := t.teamRepo.GetTeamMailConfig(ctx, params.TeamID)
	if !types.IsNil(err) {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return merr.ErrorI18nNotificationSystemError(ctx).WithCause(err)
		}
		// 配置不存在，创建新配置
		return t.teamRepo.CreateTeamMailConfig(ctx, params)
	}
	return t.teamRepo.UpdateTeamMailConfig(ctx, params)
}
