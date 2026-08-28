package mongostarter

import (
	"time"

	"github.com/acexy/golang-toolkit/util/coll"
	"github.com/acexy/golang-toolkit/util/json"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Model 定义 MongoDB 文档对应的集合名称。
type Model interface {
	CollectionName() string
}

// BaseMapper 接口声明
type BaseMapper[T Model] struct {
	model T
}

// OrderBy 排序规则
type OrderBy struct {
	// 列名
	Column string
	// 是否降序
	Desc bool
}

// QueryOptions 定义普通列表查询的排序、投影和数量限制。
type QueryOptions struct {
	OrderBy       []*OrderBy
	SelectColumns []string
	Limit         int
}

// PageOptions 定义分页以及原生查询选项。
type PageOptions struct {
	Number        int
	Size          int
	OrderBy       []*OrderBy
	SelectColumns []string
	FindOptions   []options.Lister[options.FindOptions]
	CountOptions  []options.Lister[options.CountOptions]
}

// PageQuery 定义包含实体条件的分页查询。
type PageQuery[T Model] struct {
	Condition T
	PageOptions
}

// BSONPageQuery 定义包含 BSON 条件的分页查询。
type BSONPageQuery struct {
	Condition bson.M
	PageOptions
}

// FilterPageQuery 定义包含原生 Filter 的分页查询。
type FilterPageQuery struct {
	Filter any
	PageOptions
}

// CondQuery 定义实体条件查询。
type CondQuery[T Model] struct {
	Condition T
	QueryOptions
}

// BSONQuery 定义 BSON 条件查询。
type BSONQuery struct {
	Condition bson.M
	QueryOptions
}

// NewOrderBy 新增排序规则
func NewOrderBy(column string, desc bool) []*OrderBy {
	return []*OrderBy{{Column: column, Desc: desc}}
}

// NewOrderBys 按传入顺序创建多个排序规则
func NewOrderBys(orderBy ...OrderBy) []*OrderBy {
	return coll.SliceCollect(orderBy, func(order OrderBy) *OrderBy {
		result := order
		return &result
	})
}

// Timestamp 将 MongoDB ISODate 与时间戳 JSON 相互转换。
type Timestamp json.Timestamp

func NewTimestamp(time time.Time) Timestamp {
	return Timestamp{Time: time}
}

func (t Timestamp) MarshalBSONValue() (typ byte, data []byte, err error) {
	if t.IsZero() {
		return byte(bson.TypeNull), nil, nil
	}
	valueType, data, err := bson.MarshalValue(bson.DateTime(t.Time.UnixMilli()))
	typ = byte(valueType)
	return
}

func (t *Timestamp) UnmarshalBSONValue(typ byte, data []byte) error {
	if bson.Type(typ) == bson.TypeNull {
		*t = Timestamp{Time: time.Time{}}
		return nil
	}
	var dataTime time.Time
	err := bson.UnmarshalValue(bson.Type(typ), data, &dataTime)
	if err != nil {
		return err
	}
	t.Time = dataTime
	return nil
}

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Time2Timestamp(t.Time)
}

func (t *Timestamp) UnmarshalJSON(data []byte) error {
	formatTime, err := json.Timestamp2Time(data)
	if err != nil {
		return err
	}
	t.Time = formatTime
	return nil
}

// RawMapper 提供当前 Mapper 对应的原始 Collection。
type RawMapper interface {
	// Collection 获取当前 Mapper 对应的原始 Collection
	Collection() *mongo.Collection
}

// QueryMapper 提供查询、统计和分页能力。
type QueryMapper[T Model] interface {
	// SelectByID 通过主键查询数据，默认将字符串 ID 转换为 ObjectID；普通字符串 ID 需要将 notObjectID 设置为 true
	SelectByID(id any, result *T, notObjectID ...bool) error

	// SelectByIDs 通过多个主键查询数据，默认将字符串 ID 转换为 ObjectID；普通字符串 ID 需要将 notObjectID 设置为 true
	SelectByIDs(ids []any, result *[]*T, notObjectID ...bool) (err error)

	// ExistsByID 判断指定主键的数据是否存在
	ExistsByID(id any, notObjectID ...bool) (bool, error)

	// SelectOneByCond 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectOneByCond(query CondQuery[T], result *T) error

	// SelectOneByBSON 通过 BSON 条件查询一条数据
	// specifyColumns 需要指定只查询的数据库字段
	SelectOneByBSON(query BSONQuery, result *T) error

	// SelectOneWithOptions 使用原生 FindOneOptions 查询一条数据
	SelectOneWithOptions(filter any, result *T, opts ...options.Lister[options.FindOneOptions]) error

	// SelectByCond 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectByCond(query CondQuery[T], result *[]*T) error

	// SelectByBSON 通过 BSON 条件查询数据
	// specifyColumns 需要指定只查询的数据库字段
	SelectByBSON(query BSONQuery, result *[]*T) error

	// SelectWithOptions 使用原生 FindOptions 查询数据
	SelectWithOptions(filter any, result *[]*T, opts ...options.Lister[options.FindOptions]) error

	// CountByCond 通过实体条件统计数据。
	CountByCond(query CondQuery[T]) (int64, error)

	// CountByBSON 通过 BSON 条件统计数据。
	CountByBSON(query BSONQuery) (int64, error)

	// CountWithOptions 使用原生 CountOptions 统计数据总数
	CountWithOptions(filter any, opts ...options.Lister[options.CountOptions]) (int64, error)

	// SelectPageByCond 通过实体条件分页查询
	SelectPageByCond(query PageQuery[T], result *[]*T) (total int64, err error)

	// SelectPageByBSON 通过 BSON 条件分页查询
	SelectPageByBSON(query BSONPageQuery, result *[]*T) (total int64, err error)

	// SelectPageWithOptions 使用原生查询选项分页查询
	SelectPageWithOptions(query FilterPageQuery, result *[]*T) (total int64, err error)
}

// InsertMapper 提供插入能力。
type InsertMapper[T Model] interface {
	// Insert 保存数据
	Insert(entity *T) (string, error)

	// InsertWithBSON 使用 BSON 文档插入数据
	InsertWithBSON(entity bson.M) (string, error)

	// InsertWithOptions 使用原生 InsertOneOptions 插入数据
	InsertWithOptions(document any, opts ...options.Lister[options.InsertOneOptions]) (string, error)

	// InsertBatch 批量保存数据
	InsertBatch(entities []*T) ([]string, error)

	// InsertBatchWithBSON 使用 BSON 文档批量插入数据
	InsertBatchWithBSON(entities bson.A) ([]string, error)

	// InsertBatchWithOptions 使用原生 InsertManyOptions 批量插入数据
	InsertBatchWithOptions(documents any, opts ...options.Lister[options.InsertManyOptions]) ([]string, error)
}

// UpdateMapper 提供更新能力，返回实际修改的文档数量。
type UpdateMapper[T Model] interface {
	// UpdateByID 根据主键更新数据
	UpdateByID(update *T, id any, notObjectID ...bool) (int64, error)

	// UpdateByIDWithBSON 根据主键使用 BSON 文档更新数据
	UpdateByIDWithBSON(update bson.M, id any, notObjectID ...bool) (int64, error)

	// UpdateOneByCond 通过条件更新单条数据
	UpdateOneByCond(update *T, condition T) (int64, error)

	// UpdateOneByBSON 通过 BSON 条件更新一条数据
	UpdateOneByBSON(update, condition bson.M) (int64, error)

	// UpdateByCond 通过条件更新多条数据
	UpdateByCond(update *T, condition T) (int64, error)

	// UpdateByBSON 通过 BSON 条件更新多条数据
	UpdateByBSON(update, condition bson.M) (int64, error)

	// UpdateOneWithOptions 使用原生 UpdateOneOptions 更新单条数据
	UpdateOneWithOptions(filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (int64, error)

	// UpdateWithOptions 使用原生 UpdateManyOptions 更新多条数据
	UpdateWithOptions(filter, update any, opts ...options.Lister[options.UpdateManyOptions]) (int64, error)
}

// DeleteMapper 提供删除能力，返回实际删除的文档数量。
type DeleteMapper[T Model] interface {
	// DeleteByID 根据主键删除数据
	DeleteByID(id any, notObjectID ...bool) (int64, error)

	// DeleteByIDs 根据多个主键删除数据
	DeleteByIDs(ids []any, notObjectID ...bool) (int64, error)

	// DeleteOneByCond 通过条件删除数据
	DeleteOneByCond(condition T) (int64, error)

	// DeleteOneByBSON 通过 BSON 条件删除一条数据
	DeleteOneByBSON(condition bson.M) (int64, error)

	// DeleteByCond 通过条件删除数据
	DeleteByCond(condition T) (int64, error)

	// DeleteByBSON 通过 BSON 条件删除多条数据
	DeleteByBSON(condition bson.M) (int64, error)

	// DeleteOneWithOptions 使用原生 DeleteOneOptions 删除单条数据
	DeleteOneWithOptions(filter any, opts ...options.Lister[options.DeleteOneOptions]) (int64, error)

	// DeleteWithOptions 使用原生 DeleteManyOptions 删除多条数据
	DeleteWithOptions(filter any, opts ...options.Lister[options.DeleteManyOptions]) (int64, error)
}

// Mapper 聚合原始 Collection、查询、插入、更新和删除能力。
type Mapper[T Model] interface {
	RawMapper
	QueryMapper[T]
	InsertMapper[T]
	UpdateMapper[T]
	DeleteMapper[T]
}
