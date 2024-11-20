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

func (b *BaseMapper[T]) Collection() *mongo.Collection {
	return collection(b.Value.CollectionName())
}

func (b *BaseMapper[T]) SelectById(id string, result *T, notObjectId ...bool) error {
	var queryId interface{}
	if len(notObjectId) > 0 && notObjectId[0] {
		queryId = id
	} else {
		hex, err := bson.ObjectIDFromHex(id)
		if err != nil {
			return err
		}
		queryId = hex
	}
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), bson.M{"_id": queryId}), result)
}

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

func (b *BaseMapper[T]) SelectOneByCond(condition *T, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

func (b *BaseMapper[T]) SelectOneByBson(condition bson.M, result *T, specifyColumns ...string) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

func (b *BaseMapper[T]) SelectOneByCollection(filter interface{}, result *T, opts ...options.Lister[options.FindOneOptions]) error {
	return checkSingleResult(collection(b.Value.CollectionName()).FindOne(context.Background(), filter, opts...), result)
}

func (b *BaseMapper[T]) SelectByCond(condition *T, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

func (b *BaseMapper[T]) SelectByBson(condition bson.M, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

func (b *BaseMapper[T]) SelectByCollection(filter interface{}, result *[]*T, opts ...options.Lister[options.FindOptions]) error {
	cursor, err := collection(b.Value.CollectionName()).Find(context.Background(), filter, opts...)
	return checkMultipleResult(cursor, err, result)
}

func (b *BaseMapper[T]) CountByCond(condition *T) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), condition)
}

func (b *BaseMapper[T]) CountByBson(condition bson.M) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), condition)
}

func (b *BaseMapper[T]) CountByCollection(filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return collection(b.Value.CollectionName()).CountDocuments(context.Background(), filter, opts...)
}

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

func (b *BaseMapper[T]) Save(entity *T) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), entity))
}

func (b *BaseMapper[T]) SaveByBson(entity bson.M) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), entity))
}

func (b *BaseMapper[T]) SaveByCollection(document interface{}, opts ...options.Lister[options.InsertOneOptions]) (string, error) {
	return checkSingleSaveResult(collection(b.Value.CollectionName()).InsertOne(context.Background(), document, opts...))
}

func (b *BaseMapper[T]) SaveBatch(entity *[]*T) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), *entity))
}

func (b *BaseMapper[T]) SaveBatchByBson(entity *[]*bson.M) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), *entity))
}

func (b *BaseMapper[T]) SaveBatchByCollection(documents interface{}, opts ...options.Lister[options.InsertManyOptions]) ([]string, error) {
	return checkMultipleSaveResult(collection(b.Value.CollectionName()).InsertMany(context.Background(), documents, opts...))
}

func (b *BaseMapper[T]) UpdateById(update *T, id string) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateByID(context.Background(), hex, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) UpdateByIdUseBson(update bson.M, id string) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateByID(context.Background(), hex, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) UpdateOneByCond(update, condition *T) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) UpdateOneByCondUseBson(update, condition bson.M) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) UpdateByCond(update, condition *T) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) UpdateByCondUseBson(update, condition bson.M) (bool, error) {
	return checkUpdateResult(collection(b.Value.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

func (b *BaseMapper[T]) DeleteById(id string) (bool, error) {
	hex, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return false, err
	}
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), bson.M{"_id": hex}))
}

func (b *BaseMapper[T]) DeleteOneByCond(condition *T) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), condition))
}

func (b *BaseMapper[T]) DeleteOneByCondUseBson(condition bson.M) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteOne(context.Background(), condition))
}

func (b *BaseMapper[T]) DeleteByCond(condition *T) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteMany(context.Background(), condition))
}

func (b *BaseMapper[T]) DeleteByCondUseBson(condition bson.M) (bool, error) {
	return checkDeleteResult(collection(b.Value.CollectionName()).DeleteMany(context.Background(), condition))
}
