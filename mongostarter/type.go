package mongostarter

import (
	"github.com/acexy/golang-toolkit/util/coll"
	"github.com/acexy/golang-toolkit/util/json"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"time"
)

type IBaseModel interface {
	CollectionName() string
}

// BaseMapper 接口声明
type BaseMapper[T IBaseModel] struct {
	model T
}

// OrderBy 排序规则
type OrderBy struct {
	// 列名
	Column string
	// 是否降序
	Desc bool
}

// NewOrderBy 新增排序规则
func NewOrderBy(column string, desc bool) []*OrderBy {
	return []*OrderBy{{Column: column, Desc: desc}}
}

// NewOrderBys 新增多个排序规则
func NewOrderBys(orderBy map[string]bool) []*OrderBy {
	if len(orderBy) > 0 {
		return coll.MapFilterToSlice(orderBy, func(key string, value bool) (*OrderBy, bool) {
			return &OrderBy{Column: key, Desc: value}, true
		})
	}
	return nil
}

// Timestamp 对应处理mongo数据类型为IOSDate 转换为时间戳
type Timestamp json.Timestamp

func (t Timestamp) MarshalBSONValue() (typ byte, data []byte, err error) {
	if t.IsZero() {
		return typ, nil, err
	}
	valueType, data, err := bson.MarshalValue(bson.DateTime(t.Time.UnixMilli()))
	typ = byte(valueType)
	return
}

func (t *Timestamp) UnmarshalBSONValue(typ byte, data []byte) error {
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

func (t Timestamp) UnmarshalJSON(data []byte) error {
	formatTime, err := json.Timestamp2Time(data)
	if err != nil {
		return err
	}
	t.Time = formatTime
	return nil
}

type IBaseMapper[B BaseMapper[T], T IBaseModel] interface {

	// CollWithTableName 获取对应的原始Collection操作能力
	CollWithTableName() *mongo.Collection

	// SelectById 通过主键查询数据 ObjectId类型 匹配_id 当传入的类型是string 将默认让该数据转换为mongo hex 如果string类型非mongo hex 需要将notObjectId设置为true
	SelectById(id any, result *T, notObjectId ...bool) error

	// SelectByIds 通过主键查询数据 匹配_id 当传入的类型是string 将默认让该数据转换为mongo hex 如果string类型非mongo hex 需要将notObjectId设置为true
	SelectByIds(ids []any, result *[]*T, notObjectId ...bool) (err error)

	// SelectOneByCond 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectOneByCond(condition *T, result *T, specifyColumns ...string) error

	// SelectOneByBson 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectOneByBson(condition bson.M, result *T, specifyColumns ...string) error

	// SelectOneByColl 通过原生Collection查询能力
	SelectOneByColl(filter interface{}, result *T, opts ...options.Lister[options.FindOneOptions]) error

	// SelectByCond 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectByCond(condition *T, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error

	// SelectByBson 通过条件查询
	// specifyColumns 需要指定只查询的数据库字段
	SelectByBson(condition bson.M, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error

	// SelectByColl 通过原生Collection查询能力
	SelectByColl(filter interface{}, result *[]*T, opts ...options.Lister[options.FindOptions]) error

	// CountByCond 通过条件查询数据总数
	CountByCond(condition *T) (int64, error)

	// CountByBson 通过条件查询数据总数
	CountByBson(condition bson.M) (int64, error)

	// CountByColl 通过原生Collection查询能力
	CountByColl(filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error)

	// SelectPageByCond 分页查询 pageNumber >= 1
	SelectPageByCond(condition *T, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, specifyColumns ...string) (total int64, err error)

	// SelectPageByBson 分页查询 pageNumber >= 1
	SelectPageByBson(condition bson.M, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, specifyColumns ...string) (total int64, err error)

	// SelectPageByColl 分页查询 pageNumber >= 1
	SelectPageByColl(filter interface{}, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, opts ...options.Lister[options.FindOptions]) (total int64, err error)

	// Insert 保存数据
	Insert(entity *T) (string, error)

	// InsertByBson 保存数据
	InsertByBson(entity bson.M) (string, error)

	// InsertWithOption 保存数据
	InsertWithOption(document interface{}, opts ...options.Lister[options.InsertOneOptions]) (string, error)

	// InsertBatch 批量保存数据
	InsertBatch(entities *[]*T) ([]string, error)

	// InsertBatchByBson 批量保存数据
	InsertBatchByBson(entities bson.A) ([]string, error)

	// InsertBatchWithOption 批量保存数据
	InsertBatchWithOption(documents interface{}, opts ...options.Lister[options.InsertManyOptions]) ([]string, error)

	// UpdateById 根据主键更新数据 id ObjectId hex
	UpdateById(update *T, id string) (bool, error)

	// UpdateByIdUseBson 根据主键更新数据
	UpdateByIdUseBson(update bson.M, id string) (bool, error)

	// UpdateOneByCond 通过条件更新单条数据
	UpdateOneByCond(update, condition *T) (bool, error)

	// UpdateOneByCondUseBson 通过条件更新单条数据
	UpdateOneByCondUseBson(update, condition bson.M) (bool, error)

	// UpdateByCond 通过条件更新多条数据
	UpdateByCond(update, condition *T) (bool, error)

	// UpdateByCondUseBson 通过条件更新单条数据
	UpdateByCondUseBson(update, condition bson.M) (bool, error)

	// DeleteById 根据主键删除数据
	DeleteById(id string) (bool, error)

	// DeleteOneByCond 通过条件删除数据
	DeleteOneByCond(condition *T) (bool, error)

	// DeleteOneByCondUseBson 通过条件删除数据
	DeleteOneByCondUseBson(condition bson.M) (bool, error)

	// DeleteByCond 通过条件删除数据
	DeleteByCond(condition *T) (bool, error)

	// DeleteByCondUseBson 通过条件删除数据
	DeleteByCondUseBson(condition bson.M) (bool, error)
}
