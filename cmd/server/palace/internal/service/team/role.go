package team

import (
	"context"

	teamapi "github.com/aide-family/moon/api/admin/team"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz"
	"github.com/aide-family/moon/cmd/server/palace/internal/service/builder"
	"github.com/aide-family/moon/pkg/util/types"
	"github.com/aide-family/moon/pkg/vobj"
)

// RoleService 角色服务
type RoleService struct {
	teamapi.UnimplementedRoleServer

	teamRoleBiz *biz.TeamRoleBiz
}

// NewRoleService 创建角色服务
func NewRoleService(teamRoleBiz *biz.TeamRoleBiz) *RoleService {
	return &RoleService{
		teamRoleBiz: teamRoleBiz,
	}
}

// CreateRole 创建角色
func (s *RoleService) CreateRole(ctx context.Context, req *teamapi.CreateRoleRequest) (*teamapi.CreateRoleReply, error) {
	params := builder.NewParamsBuild().RoleModuleBuilder().WithCreateRoleRequest(req).ToBo()
	_, err := s.teamRoleBiz.CreateTeamRole(ctx, params)
	if !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.CreateRoleReply{}, nil
}

// UpdateRole 更新角色
func (s *RoleService) UpdateRole(ctx context.Context, req *teamapi.UpdateRoleRequest) (*teamapi.UpdateRoleReply, error) {
	params := builder.NewParamsBuild().RoleModuleBuilder().WithUpdateRoleRequest(req).ToBo()
	if err := s.teamRoleBiz.UpdateTeamRole(ctx, params); !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.UpdateRoleReply{}, nil
}

// DeleteRole 删除角色
func (s *RoleService) DeleteRole(ctx context.Context, req *teamapi.DeleteRoleRequest) (*teamapi.DeleteRoleReply, error) {
	if err := s.teamRoleBiz.DeleteTeamRole(ctx, req.GetId()); !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.DeleteRoleReply{}, nil
}

// GetRole 获取角色详情
func (s *RoleService) GetRole(ctx context.Context, req *teamapi.GetRoleRequest) (*teamapi.GetRoleReply, error) {
	roleDetail, err := s.teamRoleBiz.GetTeamRole(ctx, req.GetId())
	if !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.GetRoleReply{
		Detail: builder.NewParamsBuild().RoleModuleBuilder().DoRoleBuilder().ToAPI(roleDetail),
	}, nil
}

// ListRole 获取角色列表
func (s *RoleService) ListRole(ctx context.Context, req *teamapi.ListRoleRequest) (*teamapi.ListRoleReply, error) {
	params := builder.NewParamsBuild().RoleModuleBuilder().WithListRoleRequest(req).ToBo()
	teamRoles, err := s.teamRoleBiz.ListTeamRole(ctx, params)
	if !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.ListRoleReply{
		List: builder.NewParamsBuild().RoleModuleBuilder().DoRoleBuilder().ToAPIs(teamRoles),
	}, nil
}

// UpdateRoleStatus 更新角色状态
func (s *RoleService) UpdateRoleStatus(ctx context.Context, req *teamapi.UpdateRoleStatusRequest) (*teamapi.UpdateRoleStatusReply, error) {
	if err := s.teamRoleBiz.UpdateTeamRoleStatus(ctx, vobj.Status(req.GetStatus()), req.GetId()); !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.UpdateRoleStatusReply{}, nil
}

// GetRoleSelectList 获取角色下拉列表
func (s *RoleService) GetRoleSelectList(ctx context.Context, req *teamapi.ListRoleRequest) (*teamapi.GetRoleSelectListReply, error) {
	params := builder.NewParamsBuild().RoleModuleBuilder().WithListRoleRequest(req).ToBo()
	teamRoles, err := s.teamRoleBiz.ListTeamRole(ctx, params)
	if !types.IsNil(err) {
		return nil, err
	}
	return &teamapi.GetRoleSelectListReply{
		List: builder.NewParamsBuild().RoleModuleBuilder().DoRoleBuilder().ToSelects(teamRoles),
	}, nil
}
