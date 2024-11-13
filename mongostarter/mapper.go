package mongostarter

import (
	"context"
	"errors"
	"fmt"
	"github.com/acexy/golang-toolkit/util/coll"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
func setOrderBy(opt **options.FindOptionsBuilder, orderBy []*OrderBy) {
	if *opt == nil {
		*opt = options.Find()
	}
	if len(orderBy) > 0 {
		(*opt).SetSort(coll.SliceFilterToMap(orderBy, func(orderBy *OrderBy) (string, int, bool) {
			var value int
			if orderBy.Desc {
				value = -1
			} else {
				value = 1
			}
			return orderBy.Column, value, true
		}))
	}
}

func setPage(opt **options.FindOptionsBuilder, pageNumber, pageSize int) {
	if *opt == nil {
		*opt = options.Find()
	}
	skip := (pageNumber - 1) * pageSize
	(*opt).SetSkip(int64(skip)).SetLimit(int64(pageSize))
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

func checkSingleSaveResult(result *mongo.InsertOneResult, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if !result.Acknowledged {
		return "", errors.New("not acknowledged")
	}
	objectId, ok := result.InsertedID.(bson.ObjectID)
	if ok {
		return objectId.Hex(), nil
	}
	return fmt.Sprintf("%v", result.InsertedID), nil
}

func checkMultipleSaveResult(result *mongo.InsertManyResult, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	if !result.Acknowledged {
		return nil, errors.New("not acknowledged")
	}
	objectIds := result.InsertedIDs
	var ids []string
	for _, v := range objectIds {
		objectId, ok := v.(bson.ObjectID)
		if ok {
			ids = append(ids, objectId.Hex())
		} else {
			ids = append(ids, fmt.Sprintf("%v", v))
		}
	}
	return ids, nil
}

func checkUpdateResult(result *mongo.UpdateResult, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if !result.Acknowledged {
		return false, errors.New("not acknowledged")
	}
	return result.ModifiedCount > 0, nil
}

func checkDeleteResult(result *mongo.DeleteResult, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if !result.Acknowledged {
		return false, errors.New("not acknowledged")
	}
	return result.DeletedCount > 0, nil
}

// Collection 获取对应的原始Collection操作能力
func (b *BaseMapper[T]) Collection() *mongo.Collection {
	return collection(b.Value.CollectionName())
}

// SelectById 通过主键查询数据 ObjectId类型
func (b *BaseMapper[T]) SelectById(id string, result *T) error {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), bson.M{"_id": hex}), result)
}

// SelectByIds 通过主键查询数据
func (b *BaseMapper[T]) SelectByIds(ids []string, result *[]*T) (err error) {
	if len(ids) == 0 {
		return errors.New("ids is empty")
	}
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	hexes := coll.SliceCollect(ids, func(id string) bson.ObjectID {
		hex, err := bson.ObjectIDFromHex(id)
		if err != nil {
			panic(err)
		}
		return hex
	})
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), bson.M{"_id": bson.M{"$in": hexes}})
	return checkMultipleResult(cursor, err, result)
}

// SelectOneByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByCond(condition *T, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByBson(condition bson.M, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByCollection 通过原生Collection查询能力
func (b *BaseMapper[T]) SelectOneByCollection(filter interface{}, result *T, opts ...options.Lister[options.FindOneOptions]) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), filter, opts...), result)
}

// SelectByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByCond(condition *T, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByBson(condition bson.M, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectByCollection 通过原生Collection查询能力
func (b *BaseMapper[T]) SelectByCollection(filter interface{}, result *[]*T, opts ...options.Lister[options.FindOptions]) error {
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), filter, opts...)
	return checkMultipleResult(cursor, err, result)
}

// CountByCond 通过条件查询数据总数
func (b *BaseMapper[T]) CountByCond(condition *T) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), condition)
}

// CountByBson 通过条件查询数据总数
func (b *BaseMapper[T]) CountByBson(condition bson.M) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), condition)
}

// CountByCollection 通过原生Collection查询能力
func (b *BaseMapper[T]) CountByCollection(filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), filter, opts...)
}

// SelectPageByCond 分页查询 pageNumber >= 1
func (b *BaseMapper[T]) SelectPageByCond(condition *T, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, specifyColumns ...string) (total int64, err error) {
	if pageNumber <= 0 || pageSize <= 0 {
		return 0, errors.New("pageNumber or pageSize <= 0")
	}
	total, err = b.CountByCond(condition)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	setPage(&opt, pageNumber, pageSize)
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageByBson 分页查询 pageNumber >= 1
func (b *BaseMapper[T]) SelectPageByBson(condition bson.M, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, specifyColumns ...string) (total int64, err error) {
	if pageNumber <= 0 || pageSize <= 0 {
		return 0, errors.New("pageNumber or pageSize <= 0")
	}
	total, err = b.CountByBson(condition)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	setPage(&opt, pageNumber, pageSize)
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageByCollection 分页查询 pageNumber >= 1
func (b *BaseMapper[T]) SelectPageByCollection(filter interface{}, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, opts ...options.Lister[options.FindOptions]) (total int64, err error) {
	if pageNumber <= 0 || pageSize <= 0 {
		return 0, errors.New("pageNumber or pageSize <= 0")
	}
	total, err = b.CountByCollection(filter)
	if err != nil {
		return 0, err
	}

	if len(orderBy) > 0 {
		opts = append(opts, options.Find().SetSort(coll.SliceFilterToMap(orderBy, func(orderBy *OrderBy) (string, int, bool) {
			var value int
			if orderBy.Desc {
				value = -1
			} else {
				value = 1
			}
			return orderBy.Column, value, true
		})))
	}
	skip := (pageNumber - 1) * pageSize
	opts = append(opts, options.Find().SetSkip(int64(skip)).SetLimit(int64(pageSize)))
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), filter, opts...)
	return total, checkMultipleResult(cursor, err, result)
}

// Save 保存数据
func (b *BaseMapper[T]) Save(entity *T) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), entity))
}

// SaveByBson 保存数据
func (b *BaseMapper[T]) SaveByBson(entity bson.M) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), entity))
}

// SaveByCollection 保存数据 使用Collection原生能力
func (b *BaseMapper[T]) SaveByCollection(document interface{}, opts ...options.Lister[options.InsertOneOptions]) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), document, opts...))
}

// SaveBatch 批量保存数据
func (b *BaseMapper[T]) SaveBatch(entity *[]*T) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), *entity))
}

// SaveBatchByBson 批量保存数据
func (b *BaseMapper[T]) SaveBatchByBson(entity *[]*bson.M) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), *entity))
}

// SaveBatchByCollection 批量保存数据
func (b *BaseMapper[T]) SaveBatchByCollection(documents interface{}, opts ...options.Lister[options.InsertManyOptions]) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), documents, opts...))
}

// UpdateById 根据主键更新数据 id ObjectId hex
func (b *BaseMapper[T]) UpdateById(id string, entity *T) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateByID(context.Background(), hex, bson.M{"$set": entity}))
}

// UpdateByIdUseBson 根据主键更新数据
func (b *BaseMapper[T]) UpdateByIdUseBson(id string, update bson.M) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateByID(context.Background(), hex, bson.M{"$set": update}))
}

// UpdateOneByCond 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateOneByCond(condition, update *T) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateOneByCondUseBson 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateOneByCondUseBson(condition, update bson.M) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateManyByCond 通过条件更新多条数据
func (b *BaseMapper[T]) UpdateManyByCond(condition, update *T) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateManyByCondUseBson 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateManyByCondUseBson(condition, update bson.M) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// DeleteById 根据主键删除数据
func (b *BaseMapper[T]) DeleteById(id string) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), bson.M{"_id": hex}))
}

// DeleteOneByCond 通过条件删除数据
func (b *BaseMapper[T]) DeleteOneByCond(condition *T) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), condition))
}

// DeleteOneByCondUseBson 通过条件删除数据
func (b *BaseMapper[T]) DeleteOneByCondUseBson(condition bson.M) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), condition))
}

// DeleteManyByCond 通过条件删除数据
func (b *BaseMapper[T]) DeleteManyByCond(condition *T) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteMany(context.Background(), condition))
}

// DeleteManyByCondUseBson 通过条件删除数据
func (b *BaseMapper[T]) DeleteManyByCondUseBson(condition bson.M) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteMany(context.Background(), condition))
}
