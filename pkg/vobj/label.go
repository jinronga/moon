package vobj

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/aide-family/moon/pkg/util/types"

	"golang.org/x/exp/maps"
)

const (
	// StrategyID 策略id
	StrategyID = "moon__strategy_id"
	// LevelID 策略级别id
	LevelID = "moon__level_id"
	// TeamID 团队id
	TeamID = "moon__team_id"
)

var _ sql.Scanner = (*Labels)(nil)
var _ driver.Valuer = (*Labels)(nil)

// ErrUnsupportedType 不支持的类型错误
var ErrUnsupportedType = errors.New("unsupported type")

// Labels 标签
type Labels struct {
	label map[string]string
}

// LabelsJSON map类型json
type LabelsJSON map[string]string

// JSON map类型json
type JSON map[string]any

// SlicesJSON 切片类型json
type SlicesJSON[T any] []T

// NewLabels 基于map创建Labels
func NewLabels(labels map[string]string) *Labels {
	return &Labels{label: labels}
}

// String 转json字符串
func (l *Labels) String() string {
	if types.IsNil(l) || l.label == nil {
		return "{}"
	}
	bs := strings.Builder{}
	bs.WriteString(`{`)
	labelKeys := maps.Keys(l.label)
	sort.Strings(labelKeys)
	for _, k := range labelKeys {
		bs.WriteString(`"` + k + `":"` + l.label[k] + `",`)
	}
	str := strings.TrimRight(bs.String(), ",")
	return str + "}"
}

// LabelsJSON 转json字符串
func (l LabelsJSON) String() string {
	if types.IsNil(l) {
		return "{}"
	}
	bs := strings.Builder{}
	bs.WriteString(`{`)
	labelKeys := maps.Keys(l)
	sort.Strings(labelKeys)
	for _, k := range labelKeys {
		bs.WriteString(`"` + k + `":"` + l[k] + `",`)
	}
	str := strings.TrimRight(bs.String(), ",")
	return str + "}"
}

// JSON 转json字符串
func (l JSON) String() string {
	if types.IsNil(l) {
		return "{}"
	}
	bs, _ := json.Marshal(l)
	return string(bs)
}

// SlicesJSON 转json字符串
func (l SlicesJSON[T]) String() string {
	if types.IsNil(l) {
		return "{}"
	}
	bs, _ := json.Marshal(l)
	return string(bs)
}

// Map 转map
func (l *Labels) Map() map[string]string {
	if l == nil || l.label == nil {
		return make(map[string]string)
	}

	return l.label
}

// Get 获取value
func (l *Labels) Get(key string) string {
	if l == nil || l.label == nil {
		return ""
	}
	return l.label[key]
}

// Append 追加
func (l *Labels) Append(key, val string) *Labels {
	l.label[key] = val
	return l
}

// AppendMap 追加map
func (l *Labels) AppendMap(m map[string]string) *Labels {
	for k, v := range m {
		l.label[k] = v
	}
	return l
}

// Index 索引
func (l *Labels) Index() string {
	str := strings.Builder{}
	str.WriteString("{")
	keys := maps.Keys(l.label)
	// 排序
	sort.Strings(keys)
	for _, key := range keys {
		k := key
		v := l.label[key]
		str.WriteString(`"` + k + `"`)
		str.WriteString(":")
		str.WriteString(`"` + v + `"`)
		str.WriteString(",")
	}
	return strings.TrimRight(str.String(), ",") + "}"
}

// Value 实现 driver.Valuer 接口
func (l Labels) Value() (driver.Value, error) {
	return json.Marshal(l.label)
}

// Scan 实现 sql.Scanner 接口
func (l *Labels) Scan(src any) (err error) {
	switch src.(type) {
	case []byte:
		err = json.Unmarshal(src.([]byte), &l.label)
	case string:
		err = json.Unmarshal([]byte(src.(string)), &l.label)
	default:
		err = ErrUnsupportedType
	}
	return err
}
