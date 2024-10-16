package mongostarter

import (
	"context"
	"github.com/acexy/golang-toolkit/util/coll"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//type BaseModel struct {
//	ID string `bson:"_id" json:"id"`
//}
//
//func (s *BaseModel) UnmarshalBSON(data []byte) error {
//	var raw map[string]interface{}
//	if err := bson.Unmarshal(data, &raw); err != nil {
//		return err
//	}
//
//	// 手动设置 _id 字段
//	if id, ok := raw["_id"]; ok {
//		s.ID = id.(string)
//	}
//
//	return bson.Unmarshal(data, s)
//}

type IBaseModel interface {
	CollectionName() string
}

// BaseMapper 接口声明
type BaseMapper[T IBaseModel] struct {
	Value T
}

func collection(collection string) *mongo.Collection {
	return RawCollection(collection)
}

func specifyColumnsOneOpt(specifyColumns ...string) *options.FindOneOptionsBuilder {
	if len(specifyColumns) > 0 {
		column := make(map[string]int, len(specifyColumns))
		for _, v := range specifyColumns {
			column[v] = 1
		}
		if !coll.SliceContains(specifyColumns, "_id") {
			column["_id"] = 0
		}
		return options.FindOne().SetProjection(column)
	}
	return nil
}

func specifyColumnsOpt(specifyColumns ...string) *options.FindOptionsBuilder {
	if len(specifyColumns) > 0 {
		column := make(map[string]int, len(specifyColumns))
		for _, v := range specifyColumns {
			column[v] = 1
		}
		if !coll.SliceContains(specifyColumns, "_id") {
			column["_id"] = 0
		}
		return options.Find().SetProjection(column)
	}
	return nil
}

func checkSingleResult(singleResult *mongo.SingleResult, result interface{}) error {
	if singleResult.Err() != nil {
		return singleResult.Err()
	}
	return singleResult.Decode(result)
}

func checkMultipleResult(cursor *mongo.Cursor, err error, result interface{}) error {
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	return cursor.All(context.Background(), result)
}

// SelectById 通过主键查询数据
func (b *BaseMapper[T]) SelectById(id string, result *T) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), bson.M{"_id": id}), result)
}

// SelectByIds 通过主键查询数据
func (b *BaseMapper[T]) SelectByIds(ids []string, result *[]T) error {
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), bson.M{"_id": bson.M{"$in": ids}})
	return checkMultipleResult(cursor, err, result)
}

// SelectOneByCondition 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByCondition(condition *T, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByBson(bson bson.M, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), bson, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectByCondition 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByCondition(condition *T, result *[]T, specifyColumns ...string) error {
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, specifyColumnsOpt(specifyColumns...))
	return checkMultipleResult(cursor, err, result)
}

// SelectByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByBson(bson bson.M, result *[]T, specifyColumns ...string) error {
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), bson, specifyColumnsOpt(specifyColumns...))
	return checkMultipleResult(cursor, err, result)
}
