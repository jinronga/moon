// Code generated by gorm.io/gen. DO NOT EDIT.
// Code generated by gorm.io/gen. DO NOT EDIT.
// Code generated by gorm.io/gen. DO NOT EDIT.

package model

import (
	"context"
	"encoding/json"

	"gorm.io/gen"
	"gorm.io/gorm"
)

const TableNameSysTeamMemberRole = "sys_team_member_roles"

// SysTeamMemberRole mapped from table <sys_team_member_roles>
type SysTeamMemberRole struct {
	SysTeamMemberID uint32 `gorm:"column:sys_team_member_id;type:int unsigned;not null;uniqueIndex:idx__user_id__team_id__role_id,priority:1;comment:团队用户ID" json:"sys_team_member_id"` // 团队用户ID
	SysTeamRoleID   uint32 `gorm:"column:sys_team_role_id;type:int unsigned;not null;uniqueIndex:idx__user_id__team_id__role_id,priority:2;comment:团队角色ID" json:"sys_team_role_id"`     // 团队角色ID
	ID              uint32 `gorm:"column:id;type:int;primaryKey" json:"id"`
	TeamID          int32  `gorm:"column:team_id;type:int;not null;index:idx__s__team_id,priority:1;comment:团队ID" json:"team_id"` // 团队ID
}

// String json string
func (c *SysTeamMemberRole) String() string {
	bs, _ := json.Marshal(c)
	return string(bs)
}

func (c *SysTeamMemberRole) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *SysTeamMemberRole) MarshalBinary() (data []byte, err error) {
	return json.Marshal(c)
}

// Create func
func (c *SysTeamMemberRole) Create(ctx context.Context, tx *gorm.DB) error {
	return tx.WithContext(ctx).Create(c).Error
}

// Update func
func (c *SysTeamMemberRole) Update(ctx context.Context, tx *gorm.DB, conds []gen.Condition) error {
	return tx.WithContext(ctx).Model(c).Where(conds).Updates(c).Error
}

// UpdateByID update func
func (c *SysTeamMemberRole) UpdateByID(ctx context.Context, tx *gorm.DB) error {
	return tx.WithContext(ctx).Model(c).Where("id = ?", c.ID).Updates(c).Error
}

// Delete func
func (c *SysTeamMemberRole) Delete(ctx context.Context, tx *gorm.DB, conds []gen.Condition) error {
	return tx.WithContext(ctx).Where(conds).Delete(c).Error
}

// DeleteByID delete func
func (c *SysTeamMemberRole) DeleteByID(ctx context.Context, tx *gorm.DB) error {
	return tx.WithContext(ctx).Delete(c, c.ID).Error
}

// TableName SysTeamMemberRole's table name
func (*SysTeamMemberRole) TableName() string {
	return TableNameSysTeamMemberRole
}
