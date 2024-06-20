// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v4.25.3
// source: rabbit/rule/rule.proto

package rule

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// 消息过滤器类型
type MessageFilterType int32

const (
	// 符合规则消息过滤
	MessageFilterType_MessageFilter_FILTER MessageFilterType = 0
	// 符合规则消息通过
	MessageFilterType_MessageFilter_PASS MessageFilterType = 1
)

// Enum value maps for MessageFilterType.
var (
	MessageFilterType_name = map[int32]string{
		0: "MessageFilter_FILTER",
		1: "MessageFilter_PASS",
	}
	MessageFilterType_value = map[string]int32{
		"MessageFilter_FILTER": 0,
		"MessageFilter_PASS":   1,
	}
)

func (x MessageFilterType) Enum() *MessageFilterType {
	p := new(MessageFilterType)
	*p = x
	return p
}

func (x MessageFilterType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MessageFilterType) Descriptor() protoreflect.EnumDescriptor {
	return file_rabbit_rule_rule_proto_enumTypes[0].Descriptor()
}

func (MessageFilterType) Type() protoreflect.EnumType {
	return &file_rabbit_rule_rule_proto_enumTypes[0]
}

func (x MessageFilterType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MessageFilterType.Descriptor instead.
func (MessageFilterType) EnumDescriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{0}
}

// 消息规则匹配类型
// 假设:规则为a,消息为b
type MessageMatchType int32

const (
	// 规则与消息进行全匹配,即a=b
	MessageMatchType_MessageMatchType_EQ MessageMatchType = 0
	// 规则包含消息全部标签,即a>=b
	MessageMatchType_MessageMatchType_IN MessageMatchType = 1
	// 消息包含规则全部标签,即b>=a
	MessageMatchType_MessageMatchType_CONTAIN MessageMatchType = 2
	// 规则与消息有任意标签匹配
	// exp:
	// a:[1,2,3]&b:[2,3,4]=true
	// a:[1,2,3]&b:[4,5,6]=false
	MessageMatchType_MessageMatchType_ANY MessageMatchType = 3
)

// Enum value maps for MessageMatchType.
var (
	MessageMatchType_name = map[int32]string{
		0: "MessageMatchType_EQ",
		1: "MessageMatchType_IN",
		2: "MessageMatchType_CONTAIN",
		3: "MessageMatchType_ANY",
	}
	MessageMatchType_value = map[string]int32{
		"MessageMatchType_EQ":      0,
		"MessageMatchType_IN":      1,
		"MessageMatchType_CONTAIN": 2,
		"MessageMatchType_ANY":     3,
	}
)

func (x MessageMatchType) Enum() *MessageMatchType {
	p := new(MessageMatchType)
	*p = x
	return p
}

func (x MessageMatchType) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (MessageMatchType) Descriptor() protoreflect.EnumDescriptor {
	return file_rabbit_rule_rule_proto_enumTypes[1].Descriptor()
}

func (MessageMatchType) Type() protoreflect.EnumType {
	return &file_rabbit_rule_rule_proto_enumTypes[1]
}

func (x MessageMatchType) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use MessageMatchType.Descriptor instead.
func (MessageMatchType) EnumDescriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{1}
}

type FilterRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息过滤器类型
	FilterType MessageFilterType `protobuf:"varint,1,opt,name=filterType,proto3,enum=api.rabbit.rule.MessageFilterType" json:"filterType,omitempty"`
	// 消息匹配类型
	MatchType MessageMatchType `protobuf:"varint,2,opt,name=matchType,proto3,enum=api.rabbit.rule.MessageMatchType" json:"matchType,omitempty"`
	// 需要匹配的标签
	MatchLabel []string `protobuf:"bytes,3,rep,name=matchLabel,proto3" json:"matchLabel,omitempty"`
	// 扩展配置，可以用于存放一下特殊配置
	Extra map[string]string `protobuf:"bytes,4,rep,name=extra,proto3" json:"extra,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *FilterRule) Reset() {
	*x = FilterRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FilterRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FilterRule) ProtoMessage() {}

func (x *FilterRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FilterRule.ProtoReflect.Descriptor instead.
func (*FilterRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{0}
}

func (x *FilterRule) GetFilterType() MessageFilterType {
	if x != nil {
		return x.FilterType
	}
	return MessageFilterType_MessageFilter_FILTER
}

func (x *FilterRule) GetMatchType() MessageMatchType {
	if x != nil {
		return x.MatchType
	}
	return MessageMatchType_MessageMatchType_EQ
}

func (x *FilterRule) GetMatchLabel() []string {
	if x != nil {
		return x.MatchLabel
	}
	return nil
}

func (x *FilterRule) GetExtra() map[string]string {
	if x != nil {
		return x.Extra
	}
	return nil
}

// 消息过滤规则
type MessageFilterRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息过滤规则名称
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 使用的过滤器名称
	Use string `protobuf:"bytes,2,opt,name=use,proto3" json:"use,omitempty"`
	// 过滤规则
	Rule *FilterRule `protobuf:"bytes,3,opt,name=rule,proto3" json:"rule,omitempty"`
}

func (x *MessageFilterRule) Reset() {
	*x = MessageFilterRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageFilterRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageFilterRule) ProtoMessage() {}

func (x *MessageFilterRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageFilterRule.ProtoReflect.Descriptor instead.
func (*MessageFilterRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{1}
}

func (x *MessageFilterRule) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *MessageFilterRule) GetUse() string {
	if x != nil {
		return x.Use
	}
	return ""
}

func (x *MessageFilterRule) GetRule() *FilterRule {
	if x != nil {
		return x.Rule
	}
	return nil
}

type AggregationRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 聚合时,每个包中，至多包含的消息数量
	Count uint32 `protobuf:"varint,1,opt,name=count,proto3" json:"count,omitempty"`
	// 发送消息的最大间隔，
	Interval *durationpb.Duration `protobuf:"bytes,2,opt,name=interval,proto3" json:"interval,omitempty"`
	// 消息聚合时用于聚合的字段
	GroupBy string `protobuf:"bytes,3,opt,name=groupBy,proto3" json:"groupBy,omitempty"`
}

func (x *AggregationRule) Reset() {
	*x = AggregationRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AggregationRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AggregationRule) ProtoMessage() {}

func (x *AggregationRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AggregationRule.ProtoReflect.Descriptor instead.
func (*AggregationRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{2}
}

func (x *AggregationRule) GetCount() uint32 {
	if x != nil {
		return x.Count
	}
	return 0
}

func (x *AggregationRule) GetInterval() *durationpb.Duration {
	if x != nil {
		return x.Interval
	}
	return nil
}

func (x *AggregationRule) GetGroupBy() string {
	if x != nil {
		return x.GroupBy
	}
	return ""
}

// 消息聚合规则
type MessageAggregationRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息聚合规则名称
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 使用的聚合器名称
	Use string `protobuf:"bytes,2,opt,name=use,proto3" json:"use,omitempty"`
	// 聚合规则
	Rule *AggregationRule `protobuf:"bytes,3,opt,name=rule,proto3" json:"rule,omitempty"`
}

func (x *MessageAggregationRule) Reset() {
	*x = MessageAggregationRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageAggregationRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageAggregationRule) ProtoMessage() {}

func (x *MessageAggregationRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageAggregationRule.ProtoReflect.Descriptor instead.
func (*MessageAggregationRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{3}
}

func (x *MessageAggregationRule) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *MessageAggregationRule) GetUse() string {
	if x != nil {
		return x.Use
	}
	return ""
}

func (x *MessageAggregationRule) GetRule() *AggregationRule {
	if x != nil {
		return x.Rule
	}
	return nil
}

// 消息模版规则
type SendRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 扩展配置，用于存放sender需要的密钥
	Config map[string]string `protobuf:"bytes,2,rep,name=config,proto3" json:"config,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *SendRule) Reset() {
	*x = SendRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SendRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SendRule) ProtoMessage() {}

func (x *SendRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SendRule.ProtoReflect.Descriptor instead.
func (*SendRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{4}
}

func (x *SendRule) GetConfig() map[string]string {
	if x != nil {
		return x.Config
	}
	return nil
}

// 消息发送规则
type MessageSendRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息发送规则名称
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 发送者名称
	Use string `protobuf:"bytes,2,opt,name=use,proto3" json:"use,omitempty"`
	// 发送规则
	Rule *SendRule `protobuf:"bytes,3,opt,name=rule,proto3" json:"rule,omitempty"`
}

func (x *MessageSendRule) Reset() {
	*x = MessageSendRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageSendRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageSendRule) ProtoMessage() {}

func (x *MessageSendRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageSendRule.ProtoReflect.Descriptor instead.
func (*MessageSendRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{5}
}

func (x *MessageSendRule) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *MessageSendRule) GetUse() string {
	if x != nil {
		return x.Use
	}
	return ""
}

func (x *MessageSendRule) GetRule() *SendRule {
	if x != nil {
		return x.Rule
	}
	return nil
}

// 消息模版规则
type TemplateRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 模版
	Template string `protobuf:"bytes,1,opt,name=template,proto3" json:"template,omitempty"`
}

func (x *TemplateRule) Reset() {
	*x = TemplateRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TemplateRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TemplateRule) ProtoMessage() {}

func (x *TemplateRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TemplateRule.ProtoReflect.Descriptor instead.
func (*TemplateRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{6}
}

func (x *TemplateRule) GetTemplate() string {
	if x != nil {
		return x.Template
	}
	return ""
}

// 消息模版规则
type MessageTemplateRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息模版规则名称
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 使用的模板解析器名称
	Use string `protobuf:"bytes,2,opt,name=use,proto3" json:"use,omitempty"`
	// 模板解析规则
	Rule *TemplateRule `protobuf:"bytes,3,opt,name=rule,proto3" json:"rule,omitempty"`
}

func (x *MessageTemplateRule) Reset() {
	*x = MessageTemplateRule{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MessageTemplateRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MessageTemplateRule) ProtoMessage() {}

func (x *MessageTemplateRule) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MessageTemplateRule.ProtoReflect.Descriptor instead.
func (*MessageTemplateRule) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{7}
}

func (x *MessageTemplateRule) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *MessageTemplateRule) GetUse() string {
	if x != nil {
		return x.Use
	}
	return ""
}

func (x *MessageTemplateRule) GetRule() *TemplateRule {
	if x != nil {
		return x.Rule
	}
	return nil
}

// 规则组
type RuleGroup struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 规则组名称
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// 消息过滤规则名称
	FilterRuleName string `protobuf:"bytes,2,opt,name=filterRuleName,proto3" json:"filterRuleName,omitempty"`
	// 消息聚合规则
	AggregationRuleName string `protobuf:"bytes,3,opt,name=aggregationRuleName,proto3" json:"aggregationRuleName,omitempty"`
	// 模版规则名称
	TemplateRuleName string `protobuf:"bytes,4,opt,name=templateRuleName,proto3" json:"templateRuleName,omitempty"`
	// 发送规则名称
	SendRuleName string `protobuf:"bytes,5,opt,name=sendRuleName,proto3" json:"sendRuleName,omitempty"`
}

func (x *RuleGroup) Reset() {
	*x = RuleGroup{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RuleGroup) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RuleGroup) ProtoMessage() {}

func (x *RuleGroup) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RuleGroup.ProtoReflect.Descriptor instead.
func (*RuleGroup) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{8}
}

func (x *RuleGroup) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *RuleGroup) GetFilterRuleName() string {
	if x != nil {
		return x.FilterRuleName
	}
	return ""
}

func (x *RuleGroup) GetAggregationRuleName() string {
	if x != nil {
		return x.AggregationRuleName
	}
	return ""
}

func (x *RuleGroup) GetTemplateRuleName() string {
	if x != nil {
		return x.TemplateRuleName
	}
	return ""
}

func (x *RuleGroup) GetSendRuleName() string {
	if x != nil {
		return x.SendRuleName
	}
	return ""
}

type Message struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// 消息ID
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// 消息标签
	Labels []string `protobuf:"bytes,2,rep,name=labels,proto3" json:"labels,omitempty"`
	// 使用的规则组名称
	UseGroups []string `protobuf:"bytes,3,rep,name=useGroups,proto3" json:"useGroups,omitempty"`
	// 消息内容
	Content map[string]*anypb.Any `protobuf:"bytes,4,rep,name=content,proto3" json:"content,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Message) Reset() {
	*x = Message{}
	if protoimpl.UnsafeEnabled {
		mi := &file_rabbit_rule_rule_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Message) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Message) ProtoMessage() {}

func (x *Message) ProtoReflect() protoreflect.Message {
	mi := &file_rabbit_rule_rule_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Message.ProtoReflect.Descriptor instead.
func (*Message) Descriptor() ([]byte, []int) {
	return file_rabbit_rule_rule_proto_rawDescGZIP(), []int{9}
}

func (x *Message) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Message) GetLabels() []string {
	if x != nil {
		return x.Labels
	}
	return nil
}

func (x *Message) GetUseGroups() []string {
	if x != nil {
		return x.UseGroups
	}
	return nil
}

func (x *Message) GetContent() map[string]*anypb.Any {
	if x != nil {
		return x.Content
	}
	return nil
}

var File_rabbit_rule_rule_proto protoreflect.FileDescriptor

var file_rabbit_rule_rule_proto_rawDesc = []byte{
	0x0a, 0x16, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2f, 0x72, 0x75, 0x6c, 0x65, 0x2f, 0x72, 0x75,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0f, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61,
	0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xa9, 0x02, 0x0a, 0x0a, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x52,
	0x75, 0x6c, 0x65, 0x12, 0x42, 0x0a, 0x0a, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70,
	0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x22, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61,
	0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0a, 0x66, 0x69, 0x6c,
	0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x3f, 0x0a, 0x09, 0x6d, 0x61, 0x74, 0x63, 0x68,
	0x54, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x21, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x4d, 0x65, 0x73,
	0x73, 0x61, 0x67, 0x65, 0x4d, 0x61, 0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65, 0x52, 0x09, 0x6d,
	0x61, 0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1e, 0x0a, 0x0a, 0x6d, 0x61, 0x74, 0x63,
	0x68, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0a, 0x6d, 0x61,
	0x74, 0x63, 0x68, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x12, 0x3c, 0x0a, 0x05, 0x65, 0x78, 0x74, 0x72,
	0x61, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x26, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61,
	0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72,
	0x52, 0x75, 0x6c, 0x65, 0x2e, 0x45, 0x78, 0x74, 0x72, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52,
	0x05, 0x65, 0x78, 0x74, 0x72, 0x61, 0x1a, 0x38, 0x0a, 0x0a, 0x45, 0x78, 0x74, 0x72, 0x61, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x22, 0x6a, 0x0a, 0x11, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x46, 0x69, 0x6c, 0x74, 0x65,
	0x72, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x73, 0x65,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x73, 0x65, 0x12, 0x2f, 0x0a, 0x04, 0x72,
	0x75, 0x6c, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x46, 0x69, 0x6c, 0x74,
	0x65, 0x72, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x22, 0x78, 0x0a, 0x0f,
	0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x75, 0x6c, 0x65, 0x12,
	0x14, 0x0a, 0x05, 0x63, 0x6f, 0x75, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05,
	0x63, 0x6f, 0x75, 0x6e, 0x74, 0x12, 0x35, 0x0a, 0x08, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61,
	0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x08, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x12, 0x18, 0x0a, 0x07,
	0x67, 0x72, 0x6f, 0x75, 0x70, 0x42, 0x79, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x67,
	0x72, 0x6f, 0x75, 0x70, 0x42, 0x79, 0x22, 0x74, 0x0a, 0x16, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x75, 0x6c, 0x65,
	0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x75, 0x73, 0x65, 0x12, 0x34, 0x0a, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x20, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61, 0x62, 0x62, 0x69,
	0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x22, 0x84, 0x01, 0x0a,
	0x08, 0x53, 0x65, 0x6e, 0x64, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x3d, 0x0a, 0x06, 0x63, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x61, 0x70, 0x69, 0x2e,
	0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x65, 0x6e, 0x64,
	0x52, 0x75, 0x6c, 0x65, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x52, 0x06, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x1a, 0x39, 0x0a, 0x0b, 0x43, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a,
	0x02, 0x38, 0x01, 0x22, 0x66, 0x0a, 0x0f, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x53, 0x65,
	0x6e, 0x64, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x73,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x73, 0x65, 0x12, 0x2d, 0x0a, 0x04,
	0x72, 0x75, 0x6c, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x65, 0x6e,
	0x64, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x22, 0x2a, 0x0a, 0x0c, 0x54,
	0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x74,
	0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x74,
	0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x22, 0x6e, 0x0a, 0x13, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x73, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x03, 0x75, 0x73, 0x65, 0x12, 0x31, 0x0a, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e,
	0x72, 0x75, 0x6c, 0x65, 0x2e, 0x54, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x75, 0x6c,
	0x65, 0x52, 0x04, 0x72, 0x75, 0x6c, 0x65, 0x22, 0xc9, 0x01, 0x0a, 0x09, 0x52, 0x75, 0x6c, 0x65,
	0x47, 0x72, 0x6f, 0x75, 0x70, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x26, 0x0a, 0x0e, 0x66, 0x69, 0x6c,
	0x74, 0x65, 0x72, 0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0e, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x30, 0x0a, 0x13, 0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x13,
	0x61, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x75, 0x6c, 0x65, 0x4e,
	0x61, 0x6d, 0x65, 0x12, 0x2a, 0x0a, 0x10, 0x74, 0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52,
	0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x74,
	0x65, 0x6d, 0x70, 0x6c, 0x61, 0x74, 0x65, 0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x12,
	0x22, 0x0a, 0x0c, 0x73, 0x65, 0x6e, 0x64, 0x52, 0x75, 0x6c, 0x65, 0x4e, 0x61, 0x6d, 0x65, 0x18,
	0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x73, 0x65, 0x6e, 0x64, 0x52, 0x75, 0x6c, 0x65, 0x4e,
	0x61, 0x6d, 0x65, 0x22, 0xe2, 0x01, 0x0a, 0x07, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x12,
	0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12,
	0x16, 0x0a, 0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52,
	0x06, 0x6c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x12, 0x1c, 0x0a, 0x09, 0x75, 0x73, 0x65, 0x47, 0x72,
	0x6f, 0x75, 0x70, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x09, 0x75, 0x73, 0x65, 0x47,
	0x72, 0x6f, 0x75, 0x70, 0x73, 0x12, 0x3f, 0x0a, 0x07, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74,
	0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x25, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61, 0x62,
	0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x2e, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x07, 0x63,
	0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x1a, 0x50, 0x0a, 0x0c, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e,
	0x74, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x2a, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x2a, 0x45, 0x0a, 0x11, 0x4d, 0x65, 0x73, 0x73,
	0x61, 0x67, 0x65, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x12, 0x18, 0x0a,
	0x14, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x5f, 0x46,
	0x49, 0x4c, 0x54, 0x45, 0x52, 0x10, 0x00, 0x12, 0x16, 0x0a, 0x12, 0x4d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x46, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x5f, 0x50, 0x41, 0x53, 0x53, 0x10, 0x01, 0x2a,
	0x7c, 0x0a, 0x10, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4d, 0x61, 0x74, 0x63, 0x68, 0x54,
	0x79, 0x70, 0x65, 0x12, 0x17, 0x0a, 0x13, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4d, 0x61,
	0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65, 0x5f, 0x45, 0x51, 0x10, 0x00, 0x12, 0x17, 0x0a, 0x13,
	0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4d, 0x61, 0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65,
	0x5f, 0x49, 0x4e, 0x10, 0x01, 0x12, 0x1c, 0x0a, 0x18, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x4d, 0x61, 0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65, 0x5f, 0x43, 0x4f, 0x4e, 0x54, 0x41, 0x49,
	0x4e, 0x10, 0x02, 0x12, 0x18, 0x0a, 0x14, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x4d, 0x61,
	0x74, 0x63, 0x68, 0x54, 0x79, 0x70, 0x65, 0x5f, 0x41, 0x4e, 0x59, 0x10, 0x03, 0x42, 0x45, 0x0a,
	0x0f, 0x61, 0x70, 0x69, 0x2e, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2e, 0x72, 0x75, 0x6c, 0x65,
	0x50, 0x01, 0x5a, 0x30, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61,
	0x69, 0x64, 0x65, 0x2d, 0x66, 0x61, 0x6d, 0x69, 0x6c, 0x79, 0x2f, 0x6d, 0x6f, 0x6f, 0x6e, 0x2f,
	0x61, 0x70, 0x69, 0x2f, 0x72, 0x61, 0x62, 0x62, 0x69, 0x74, 0x2f, 0x72, 0x75, 0x6c, 0x65, 0x3b,
	0x72, 0x75, 0x6c, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_rabbit_rule_rule_proto_rawDescOnce sync.Once
	file_rabbit_rule_rule_proto_rawDescData = file_rabbit_rule_rule_proto_rawDesc
)

func file_rabbit_rule_rule_proto_rawDescGZIP() []byte {
	file_rabbit_rule_rule_proto_rawDescOnce.Do(func() {
		file_rabbit_rule_rule_proto_rawDescData = protoimpl.X.CompressGZIP(file_rabbit_rule_rule_proto_rawDescData)
	})
	return file_rabbit_rule_rule_proto_rawDescData
}

var file_rabbit_rule_rule_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_rabbit_rule_rule_proto_msgTypes = make([]protoimpl.MessageInfo, 13)
var file_rabbit_rule_rule_proto_goTypes = []any{
	(MessageFilterType)(0),         // 0: api.rabbit.rule.MessageFilterType
	(MessageMatchType)(0),          // 1: api.rabbit.rule.MessageMatchType
	(*FilterRule)(nil),             // 2: api.rabbit.rule.FilterRule
	(*MessageFilterRule)(nil),      // 3: api.rabbit.rule.MessageFilterRule
	(*AggregationRule)(nil),        // 4: api.rabbit.rule.AggregationRule
	(*MessageAggregationRule)(nil), // 5: api.rabbit.rule.MessageAggregationRule
	(*SendRule)(nil),               // 6: api.rabbit.rule.SendRule
	(*MessageSendRule)(nil),        // 7: api.rabbit.rule.MessageSendRule
	(*TemplateRule)(nil),           // 8: api.rabbit.rule.TemplateRule
	(*MessageTemplateRule)(nil),    // 9: api.rabbit.rule.MessageTemplateRule
	(*RuleGroup)(nil),              // 10: api.rabbit.rule.RuleGroup
	(*Message)(nil),                // 11: api.rabbit.rule.Message
	nil,                            // 12: api.rabbit.rule.FilterRule.ExtraEntry
	nil,                            // 13: api.rabbit.rule.SendRule.ConfigEntry
	nil,                            // 14: api.rabbit.rule.Message.ContentEntry
	(*durationpb.Duration)(nil),    // 15: google.protobuf.Duration
	(*anypb.Any)(nil),              // 16: google.protobuf.Any
}
var file_rabbit_rule_rule_proto_depIdxs = []int32{
	0,  // 0: api.rabbit.rule.FilterRule.filterType:type_name -> api.rabbit.rule.MessageFilterType
	1,  // 1: api.rabbit.rule.FilterRule.matchType:type_name -> api.rabbit.rule.MessageMatchType
	12, // 2: api.rabbit.rule.FilterRule.extra:type_name -> api.rabbit.rule.FilterRule.ExtraEntry
	2,  // 3: api.rabbit.rule.MessageFilterRule.rule:type_name -> api.rabbit.rule.FilterRule
	15, // 4: api.rabbit.rule.AggregationRule.interval:type_name -> google.protobuf.Duration
	4,  // 5: api.rabbit.rule.MessageAggregationRule.rule:type_name -> api.rabbit.rule.AggregationRule
	13, // 6: api.rabbit.rule.SendRule.config:type_name -> api.rabbit.rule.SendRule.ConfigEntry
	6,  // 7: api.rabbit.rule.MessageSendRule.rule:type_name -> api.rabbit.rule.SendRule
	8,  // 8: api.rabbit.rule.MessageTemplateRule.rule:type_name -> api.rabbit.rule.TemplateRule
	14, // 9: api.rabbit.rule.Message.content:type_name -> api.rabbit.rule.Message.ContentEntry
	16, // 10: api.rabbit.rule.Message.ContentEntry.value:type_name -> google.protobuf.Any
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_rabbit_rule_rule_proto_init() }
func file_rabbit_rule_rule_proto_init() {
	if File_rabbit_rule_rule_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_rabbit_rule_rule_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*FilterRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*MessageFilterRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*AggregationRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*MessageAggregationRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[4].Exporter = func(v any, i int) any {
			switch v := v.(*SendRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[5].Exporter = func(v any, i int) any {
			switch v := v.(*MessageSendRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[6].Exporter = func(v any, i int) any {
			switch v := v.(*TemplateRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[7].Exporter = func(v any, i int) any {
			switch v := v.(*MessageTemplateRule); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[8].Exporter = func(v any, i int) any {
			switch v := v.(*RuleGroup); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_rabbit_rule_rule_proto_msgTypes[9].Exporter = func(v any, i int) any {
			switch v := v.(*Message); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_rabbit_rule_rule_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   13,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_rabbit_rule_rule_proto_goTypes,
		DependencyIndexes: file_rabbit_rule_rule_proto_depIdxs,
		EnumInfos:         file_rabbit_rule_rule_proto_enumTypes,
		MessageInfos:      file_rabbit_rule_rule_proto_msgTypes,
	}.Build()
	File_rabbit_rule_rule_proto = out.File
	file_rabbit_rule_rule_proto_rawDesc = nil
	file_rabbit_rule_rule_proto_goTypes = nil
	file_rabbit_rule_rule_proto_depIdxs = nil
}
