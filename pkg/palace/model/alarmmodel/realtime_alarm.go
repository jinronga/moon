package alarmmodel

import (
	"encoding/json"

	"github.com/aide-family/moon/pkg/palace/model"
	"github.com/aide-family/moon/pkg/vobj"
)

const tableNameRealtimeAlarm = "realtime_alarm"

// RealtimeAlarm mapped from table <RealtimeAlarm>
type RealtimeAlarm struct {
	model.EasyModel
	// 告警状态: 1告警;2恢复
	Status vobj.AlertStatus `gorm:"column:status;type:tinyint;not null;default:1;comment:告警状态: 1告警;2恢复"`
	// 告警时间
	StartsAt int64 `gorm:"column:starts_at;type:bigint;not null;comment:告警时间"`
	// 恢复时间
	EndsAt int64 `gorm:"column:ends_at;type:bigint;not null;comment:恢复时间"`
	// 原始信息json
	RawInfo string `gorm:"column:raw_info;type:text;not null;comment:原始信息json"`
	// 告警摘要
	Summary string `gorm:"column:summary;type:varchar(255);not null;comment:告警摘要"`
	// 告警明细
	Description string `gorm:"column:description;type:text;not null;comment:告警明细"`
	// 触发告警表达式
	Expr string `gorm:"column:expr;type:text;not null;comment:告警表达式"`
	// 指纹
	Fingerprint string `gorm:"column:fingerprint;type:varchar(255);not null;comment:fingerprint;uniqueIndex"`
	// 实时告警详情
	RealtimeDetails RealtimeDetails `gorm:"foreignKey:RealtimeAlarmID"`
}

// String json string
func (c *RealtimeAlarm) String() string {
	bs, _ := json.Marshal(c)
	return string(bs)
}

// UnmarshalBinary redis存储实现
func (c *RealtimeAlarm) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, c)
}

// MarshalBinary redis存储实现
func (c *RealtimeAlarm) MarshalBinary() (data []byte, err error) {
	return json.Marshal(c)
}

// TableName RealtimeAlarm's table name
func (*RealtimeAlarm) TableName() string {
	return tableNameRealtimeAlarm
}
