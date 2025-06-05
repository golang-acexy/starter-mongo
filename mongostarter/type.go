package mongostarter

import (
	"github.com/acexy/golang-toolkit/util/coll"
	"github.com/acexy/golang-toolkit/util/json"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"
)

type IBaseModel interface {
	CollectionName() string
}

// BaseMapper 接口声明
type BaseMapper[T IBaseModel] struct {
	Value T
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
