package repoimpl

import (
	"context"

	"github.com/aide-family/moon/cmd/server/palace/internal/biz/bo"
	"github.com/aide-family/moon/cmd/server/palace/internal/biz/repository"
	"github.com/aide-family/moon/cmd/server/palace/internal/data"
	"github.com/aide-family/moon/pkg/palace/model/bizmodel"
	"github.com/aide-family/moon/pkg/palace/model/bizmodel/bizquery"
	"github.com/aide-family/moon/pkg/util/types"
	"github.com/aide-family/moon/pkg/vobj"

	"gorm.io/gen"
)

// NewAlarmHookRepository new alarm repository
func NewAlarmHookRepository(data *data.Data) repository.AlarmHook {
	return &alarmHookRepositoryImpl{
		data: data,
	}
}

type (
	alarmHookRepositoryImpl struct {
		data *data.Data
	}
)

func (a *alarmHookRepositoryImpl) CreateAlarmHook(ctx context.Context, params *bo.CreateAlarmHookParams) (*bizmodel.AlarmHook, error) {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return nil, err
	}

	hookModel := createAlarmHookParamsToModel(ctx, params)

	err = bizQuery.AlarmHook.WithContext(ctx).Create(hookModel)

	if !types.IsNil(err) {
		return nil, err
	}

	return hookModel, nil
}

func (a *alarmHookRepositoryImpl) UpdateAlarmHook(ctx context.Context, params *bo.UpdateAlarmHookParams) error {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return err
	}

	updateParam := params.UpdateParam
	hookModel := createAlarmHookParamsToModel(ctx, updateParam)
	_, err = bizQuery.AlarmHook.WithContext(ctx).Where(bizQuery.AlarmHook.ID.Eq(params.ID)).Updates(hookModel)
	return err
}

func (a *alarmHookRepositoryImpl) DeleteAlarmHook(ctx context.Context, ID uint32) error {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return err
	}
	_, err = bizQuery.AlarmHook.WithContext(ctx).Where(bizQuery.AlarmHook.ID.Eq(ID)).Delete()
	return err
}

func (a *alarmHookRepositoryImpl) GetAlarmHook(ctx context.Context, ID uint32) (*bizmodel.AlarmHook, error) {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return nil, err
	}
	return bizQuery.AlarmHook.WithContext(ctx).Where(bizQuery.AlarmHook.ID.Eq(ID)).First()
}

func (a *alarmHookRepositoryImpl) ListAlarmHook(ctx context.Context, params *bo.QueryAlarmHookListParams) ([]*bizmodel.AlarmHook, error) {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return nil, err
	}
	bizWrapper := bizQuery.AlarmHook.WithContext(ctx)
	var wheres []gen.Condition
	if !types.TextIsNull(params.Name) {
		wheres = append(wheres, bizQuery.AlarmHook.Name.Like(params.Name))
	}

	if !types.TextIsNull(params.Keyword) {
		wheres = append(wheres, bizQuery.AlarmHook.Name.Like(params.Keyword))
		wheres = append(wheres, bizQuery.AlarmHook.Remark.Like(params.Keyword))
	}

	if !params.Status.IsUnknown() {
		wheres = append(wheres, bizQuery.AlarmHook.Status.Eq(params.Status.GetValue()))
	}

	if !types.IsNil(params.Apps) && len(params.Apps) > 0 {
		apps := types.SliceTo(params.Apps, func(app vobj.HookAPP) int {
			return app.GetValue()
		})
		wheres = append(wheres, bizQuery.AlarmHook.APP.In(apps...))
	}
	if err := types.WithPageQuery[bizquery.IAlarmHookDo](bizWrapper, params.Page); err != nil {
		return nil, err
	}
	return bizWrapper.Where(wheres...).Order(bizQuery.AlarmHook.ID.Desc()).Find()
}

func (a *alarmHookRepositoryImpl) UpdateAlarmHookStatus(ctx context.Context, params *bo.UpdateAlarmHookStatusParams) error {
	bizQuery, err := getBizQuery(ctx, a.data)
	if !types.IsNil(err) {
		return err
	}
	_, err = bizQuery.AlarmHook.WithContext(ctx).Where(bizQuery.AlarmHook.ID.In(params.IDs...)).Update(bizQuery.AlarmHook.Status, params.Status)
	return err
}

func createAlarmHookParamsToModel(ctx context.Context, params *bo.CreateAlarmHookParams) *bizmodel.AlarmHook {
	hookModel := &bizmodel.AlarmHook{
		Name:   params.Name,
		Remark: params.Remark,
		Status: params.Status,
		URL:    params.URL,
		Secret: params.Secret,
		APP:    params.HookApp,
	}
	hookModel.WithContext(ctx)
	return hookModel
}
