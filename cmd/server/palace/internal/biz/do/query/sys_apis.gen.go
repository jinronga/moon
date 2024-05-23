// Code generated by gorm.io/gen. DO NOT EDIT.
// Code generated by gorm.io/gen. DO NOT EDIT.
// Code generated by gorm.io/gen. DO NOT EDIT.

package query

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	"gorm.io/gen"
	"gorm.io/gen/field"

	"gorm.io/plugin/dbresolver"

	"github.com/aide-cloud/moon/cmd/server/palace/internal/biz/do/model"
)

func newSysAPI(db *gorm.DB, opts ...gen.DOOption) sysAPI {
	_sysAPI := sysAPI{}

	_sysAPI.sysAPIDo.UseDB(db, opts...)
	_sysAPI.sysAPIDo.UseModel(&model.SysAPI{})

	tableName := _sysAPI.sysAPIDo.TableName()
	_sysAPI.ALL = field.NewAsterisk(tableName)
	_sysAPI.ID = field.NewUint32(tableName, "id")
	_sysAPI.CreatedAt = field.NewField(tableName, "created_at")
	_sysAPI.UpdatedAt = field.NewField(tableName, "updated_at")
	_sysAPI.Name = field.NewString(tableName, "name")
	_sysAPI.Path = field.NewString(tableName, "path")
	_sysAPI.Status = field.NewField(tableName, "status")
	_sysAPI.Remark = field.NewString(tableName, "remark")
	_sysAPI.DeletedAt = field.NewInt64(tableName, "deleted_at")
	_sysAPI.Module = field.NewInt32(tableName, "module")
	_sysAPI.Domain = field.NewInt32(tableName, "domain")
	_sysAPI.SysTeamRoles = sysAPIManyToManySysTeamRoles{
		db: db.Session(&gorm.Session{}),

		RelationField: field.NewRelation("SysTeamRoles", "model.SysTeamRole"),
	}

	_sysAPI.fillFieldMap()

	return _sysAPI
}

type sysAPI struct {
	sysAPIDo

	ALL          field.Asterisk
	ID           field.Uint32
	CreatedAt    field.Field  // 创建时间
	UpdatedAt    field.Field  // 更新时间
	Name         field.String // api名称
	Path         field.String // api路径
	Status       field.Field  // 状态
	Remark       field.String // 备注
	DeletedAt    field.Int64
	Module       field.Int32 // 模块
	Domain       field.Int32 // 领域
	SysTeamRoles sysAPIManyToManySysTeamRoles

	fieldMap map[string]field.Expr
}

func (s sysAPI) Table(newTableName string) *sysAPI {
	s.sysAPIDo.UseTable(newTableName)
	return s.updateTableName(newTableName)
}

func (s sysAPI) As(alias string) *sysAPI {
	s.sysAPIDo.DO = *(s.sysAPIDo.As(alias).(*gen.DO))
	return s.updateTableName(alias)
}

func (s *sysAPI) updateTableName(table string) *sysAPI {
	s.ALL = field.NewAsterisk(table)
	s.ID = field.NewUint32(table, "id")
	s.CreatedAt = field.NewField(table, "created_at")
	s.UpdatedAt = field.NewField(table, "updated_at")
	s.Name = field.NewString(table, "name")
	s.Path = field.NewString(table, "path")
	s.Status = field.NewField(table, "status")
	s.Remark = field.NewString(table, "remark")
	s.DeletedAt = field.NewInt64(table, "deleted_at")
	s.Module = field.NewInt32(table, "module")
	s.Domain = field.NewInt32(table, "domain")

	s.fillFieldMap()

	return s
}

func (s *sysAPI) GetFieldByName(fieldName string) (field.OrderExpr, bool) {
	_f, ok := s.fieldMap[fieldName]
	if !ok || _f == nil {
		return nil, false
	}
	_oe, ok := _f.(field.OrderExpr)
	return _oe, ok
}

func (s *sysAPI) fillFieldMap() {
	s.fieldMap = make(map[string]field.Expr, 11)
	s.fieldMap["id"] = s.ID
	s.fieldMap["created_at"] = s.CreatedAt
	s.fieldMap["updated_at"] = s.UpdatedAt
	s.fieldMap["name"] = s.Name
	s.fieldMap["path"] = s.Path
	s.fieldMap["status"] = s.Status
	s.fieldMap["remark"] = s.Remark
	s.fieldMap["deleted_at"] = s.DeletedAt
	s.fieldMap["module"] = s.Module
	s.fieldMap["domain"] = s.Domain

}

func (s sysAPI) clone(db *gorm.DB) sysAPI {
	s.sysAPIDo.ReplaceConnPool(db.Statement.ConnPool)
	return s
}

func (s sysAPI) replaceDB(db *gorm.DB) sysAPI {
	s.sysAPIDo.ReplaceDB(db)
	return s
}

type sysAPIManyToManySysTeamRoles struct {
	db *gorm.DB

	field.RelationField
}

func (a sysAPIManyToManySysTeamRoles) Where(conds ...field.Expr) *sysAPIManyToManySysTeamRoles {
	if len(conds) == 0 {
		return &a
	}

	exprs := make([]clause.Expression, 0, len(conds))
	for _, cond := range conds {
		exprs = append(exprs, cond.BeCond().(clause.Expression))
	}
	a.db = a.db.Clauses(clause.Where{Exprs: exprs})
	return &a
}

func (a sysAPIManyToManySysTeamRoles) WithContext(ctx context.Context) *sysAPIManyToManySysTeamRoles {
	a.db = a.db.WithContext(ctx)
	return &a
}

func (a sysAPIManyToManySysTeamRoles) Session(session *gorm.Session) *sysAPIManyToManySysTeamRoles {
	a.db = a.db.Session(session)
	return &a
}

func (a sysAPIManyToManySysTeamRoles) Model(m *model.SysAPI) *sysAPIManyToManySysTeamRolesTx {
	return &sysAPIManyToManySysTeamRolesTx{a.db.Model(m).Association(a.Name())}
}

type sysAPIManyToManySysTeamRolesTx struct{ tx *gorm.Association }

func (a sysAPIManyToManySysTeamRolesTx) Find() (result []*model.SysTeamRole, err error) {
	return result, a.tx.Find(&result)
}

func (a sysAPIManyToManySysTeamRolesTx) Append(values ...*model.SysTeamRole) (err error) {
	targetValues := make([]interface{}, len(values))
	for i, v := range values {
		targetValues[i] = v
	}
	return a.tx.Append(targetValues...)
}

func (a sysAPIManyToManySysTeamRolesTx) Replace(values ...*model.SysTeamRole) (err error) {
	targetValues := make([]interface{}, len(values))
	for i, v := range values {
		targetValues[i] = v
	}
	return a.tx.Replace(targetValues...)
}

func (a sysAPIManyToManySysTeamRolesTx) Delete(values ...*model.SysTeamRole) (err error) {
	targetValues := make([]interface{}, len(values))
	for i, v := range values {
		targetValues[i] = v
	}
	return a.tx.Delete(targetValues...)
}

func (a sysAPIManyToManySysTeamRolesTx) Clear() error {
	return a.tx.Clear()
}

func (a sysAPIManyToManySysTeamRolesTx) Count() int64 {
	return a.tx.Count()
}

type sysAPIDo struct{ gen.DO }

type ISysAPIDo interface {
	gen.SubQuery
	Debug() ISysAPIDo
	WithContext(ctx context.Context) ISysAPIDo
	WithResult(fc func(tx gen.Dao)) gen.ResultInfo
	ReplaceDB(db *gorm.DB)
	ReadDB() ISysAPIDo
	WriteDB() ISysAPIDo
	As(alias string) gen.Dao
	Session(config *gorm.Session) ISysAPIDo
	Columns(cols ...field.Expr) gen.Columns
	Clauses(conds ...clause.Expression) ISysAPIDo
	Not(conds ...gen.Condition) ISysAPIDo
	Or(conds ...gen.Condition) ISysAPIDo
	Select(conds ...field.Expr) ISysAPIDo
	Where(conds ...gen.Condition) ISysAPIDo
	Order(conds ...field.Expr) ISysAPIDo
	Distinct(cols ...field.Expr) ISysAPIDo
	Omit(cols ...field.Expr) ISysAPIDo
	Join(table schema.Tabler, on ...field.Expr) ISysAPIDo
	LeftJoin(table schema.Tabler, on ...field.Expr) ISysAPIDo
	RightJoin(table schema.Tabler, on ...field.Expr) ISysAPIDo
	Group(cols ...field.Expr) ISysAPIDo
	Having(conds ...gen.Condition) ISysAPIDo
	Limit(limit int) ISysAPIDo
	Offset(offset int) ISysAPIDo
	Count() (count int64, err error)
	Scopes(funcs ...func(gen.Dao) gen.Dao) ISysAPIDo
	Unscoped() ISysAPIDo
	Create(values ...*model.SysAPI) error
	CreateInBatches(values []*model.SysAPI, batchSize int) error
	Save(values ...*model.SysAPI) error
	First() (*model.SysAPI, error)
	Take() (*model.SysAPI, error)
	Last() (*model.SysAPI, error)
	Find() ([]*model.SysAPI, error)
	FindInBatch(batchSize int, fc func(tx gen.Dao, batch int) error) (results []*model.SysAPI, err error)
	FindInBatches(result *[]*model.SysAPI, batchSize int, fc func(tx gen.Dao, batch int) error) error
	Pluck(column field.Expr, dest interface{}) error
	Delete(...*model.SysAPI) (info gen.ResultInfo, err error)
	Update(column field.Expr, value interface{}) (info gen.ResultInfo, err error)
	UpdateSimple(columns ...field.AssignExpr) (info gen.ResultInfo, err error)
	Updates(value interface{}) (info gen.ResultInfo, err error)
	UpdateColumn(column field.Expr, value interface{}) (info gen.ResultInfo, err error)
	UpdateColumnSimple(columns ...field.AssignExpr) (info gen.ResultInfo, err error)
	UpdateColumns(value interface{}) (info gen.ResultInfo, err error)
	UpdateFrom(q gen.SubQuery) gen.Dao
	Attrs(attrs ...field.AssignExpr) ISysAPIDo
	Assign(attrs ...field.AssignExpr) ISysAPIDo
	Joins(fields ...field.RelationField) ISysAPIDo
	Preload(fields ...field.RelationField) ISysAPIDo
	FirstOrInit() (*model.SysAPI, error)
	FirstOrCreate() (*model.SysAPI, error)
	FindByPage(offset int, limit int) (result []*model.SysAPI, count int64, err error)
	ScanByPage(result interface{}, offset int, limit int) (count int64, err error)
	Scan(result interface{}) (err error)
	Returning(value interface{}, columns ...string) ISysAPIDo
	UnderlyingDB() *gorm.DB
	schema.Tabler
}

func (s sysAPIDo) Debug() ISysAPIDo {
	return s.withDO(s.DO.Debug())
}

func (s sysAPIDo) WithContext(ctx context.Context) ISysAPIDo {
	return s.withDO(s.DO.WithContext(ctx))
}

func (s sysAPIDo) ReadDB() ISysAPIDo {
	return s.Clauses(dbresolver.Read)
}

func (s sysAPIDo) WriteDB() ISysAPIDo {
	return s.Clauses(dbresolver.Write)
}

func (s sysAPIDo) Session(config *gorm.Session) ISysAPIDo {
	return s.withDO(s.DO.Session(config))
}

func (s sysAPIDo) Clauses(conds ...clause.Expression) ISysAPIDo {
	return s.withDO(s.DO.Clauses(conds...))
}

func (s sysAPIDo) Returning(value interface{}, columns ...string) ISysAPIDo {
	return s.withDO(s.DO.Returning(value, columns...))
}

func (s sysAPIDo) Not(conds ...gen.Condition) ISysAPIDo {
	return s.withDO(s.DO.Not(conds...))
}

func (s sysAPIDo) Or(conds ...gen.Condition) ISysAPIDo {
	return s.withDO(s.DO.Or(conds...))
}

func (s sysAPIDo) Select(conds ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Select(conds...))
}

func (s sysAPIDo) Where(conds ...gen.Condition) ISysAPIDo {
	return s.withDO(s.DO.Where(conds...))
}

func (s sysAPIDo) Order(conds ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Order(conds...))
}

func (s sysAPIDo) Distinct(cols ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Distinct(cols...))
}

func (s sysAPIDo) Omit(cols ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Omit(cols...))
}

func (s sysAPIDo) Join(table schema.Tabler, on ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Join(table, on...))
}

func (s sysAPIDo) LeftJoin(table schema.Tabler, on ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.LeftJoin(table, on...))
}

func (s sysAPIDo) RightJoin(table schema.Tabler, on ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.RightJoin(table, on...))
}

func (s sysAPIDo) Group(cols ...field.Expr) ISysAPIDo {
	return s.withDO(s.DO.Group(cols...))
}

func (s sysAPIDo) Having(conds ...gen.Condition) ISysAPIDo {
	return s.withDO(s.DO.Having(conds...))
}

func (s sysAPIDo) Limit(limit int) ISysAPIDo {
	return s.withDO(s.DO.Limit(limit))
}

func (s sysAPIDo) Offset(offset int) ISysAPIDo {
	return s.withDO(s.DO.Offset(offset))
}

func (s sysAPIDo) Scopes(funcs ...func(gen.Dao) gen.Dao) ISysAPIDo {
	return s.withDO(s.DO.Scopes(funcs...))
}

func (s sysAPIDo) Unscoped() ISysAPIDo {
	return s.withDO(s.DO.Unscoped())
}

func (s sysAPIDo) Create(values ...*model.SysAPI) error {
	if len(values) == 0 {
		return nil
	}
	return s.DO.Create(values)
}

func (s sysAPIDo) CreateInBatches(values []*model.SysAPI, batchSize int) error {
	return s.DO.CreateInBatches(values, batchSize)
}

// Save : !!! underlying implementation is different with GORM
// The method is equivalent to executing the statement: db.Clauses(clause.OnConflict{UpdateAll: true}).Create(values)
func (s sysAPIDo) Save(values ...*model.SysAPI) error {
	if len(values) == 0 {
		return nil
	}
	return s.DO.Save(values)
}

func (s sysAPIDo) First() (*model.SysAPI, error) {
	if result, err := s.DO.First(); err != nil {
		return nil, err
	} else {
		return result.(*model.SysAPI), nil
	}
}

func (s sysAPIDo) Take() (*model.SysAPI, error) {
	if result, err := s.DO.Take(); err != nil {
		return nil, err
	} else {
		return result.(*model.SysAPI), nil
	}
}

func (s sysAPIDo) Last() (*model.SysAPI, error) {
	if result, err := s.DO.Last(); err != nil {
		return nil, err
	} else {
		return result.(*model.SysAPI), nil
	}
}

func (s sysAPIDo) Find() ([]*model.SysAPI, error) {
	result, err := s.DO.Find()
	return result.([]*model.SysAPI), err
}

func (s sysAPIDo) FindInBatch(batchSize int, fc func(tx gen.Dao, batch int) error) (results []*model.SysAPI, err error) {
	buf := make([]*model.SysAPI, 0, batchSize)
	err = s.DO.FindInBatches(&buf, batchSize, func(tx gen.Dao, batch int) error {
		defer func() { results = append(results, buf...) }()
		return fc(tx, batch)
	})
	return results, err
}

func (s sysAPIDo) FindInBatches(result *[]*model.SysAPI, batchSize int, fc func(tx gen.Dao, batch int) error) error {
	return s.DO.FindInBatches(result, batchSize, fc)
}

func (s sysAPIDo) Attrs(attrs ...field.AssignExpr) ISysAPIDo {
	return s.withDO(s.DO.Attrs(attrs...))
}

func (s sysAPIDo) Assign(attrs ...field.AssignExpr) ISysAPIDo {
	return s.withDO(s.DO.Assign(attrs...))
}

func (s sysAPIDo) Joins(fields ...field.RelationField) ISysAPIDo {
	for _, _f := range fields {
		s = *s.withDO(s.DO.Joins(_f))
	}
	return &s
}

func (s sysAPIDo) Preload(fields ...field.RelationField) ISysAPIDo {
	for _, _f := range fields {
		s = *s.withDO(s.DO.Preload(_f))
	}
	return &s
}

func (s sysAPIDo) FirstOrInit() (*model.SysAPI, error) {
	if result, err := s.DO.FirstOrInit(); err != nil {
		return nil, err
	} else {
		return result.(*model.SysAPI), nil
	}
}

func (s sysAPIDo) FirstOrCreate() (*model.SysAPI, error) {
	if result, err := s.DO.FirstOrCreate(); err != nil {
		return nil, err
	} else {
		return result.(*model.SysAPI), nil
	}
}

func (s sysAPIDo) FindByPage(offset int, limit int) (result []*model.SysAPI, count int64, err error) {
	result, err = s.Offset(offset).Limit(limit).Find()
	if err != nil {
		return
	}

	if size := len(result); 0 < limit && 0 < size && size < limit {
		count = int64(size + offset)
		return
	}

	count, err = s.Offset(-1).Limit(-1).Count()
	return
}

func (s sysAPIDo) ScanByPage(result interface{}, offset int, limit int) (count int64, err error) {
	count, err = s.Count()
	if err != nil {
		return
	}

	err = s.Offset(offset).Limit(limit).Scan(result)
	return
}

func (s sysAPIDo) Scan(result interface{}) (err error) {
	return s.DO.Scan(result)
}

func (s sysAPIDo) Delete(models ...*model.SysAPI) (result gen.ResultInfo, err error) {
	return s.DO.Delete(models)
}

func (s *sysAPIDo) withDO(do gen.Dao) *sysAPIDo {
	s.DO = *do.(*gen.DO)
	return s
}
