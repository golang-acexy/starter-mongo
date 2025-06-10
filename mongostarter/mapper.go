package mongostarter

import (
	"context"
	"errors"
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

// CollWithTableName 获取对应的原始Collection操作能力 已限定集合名
func (b *BaseMapper[T]) CollWithTableName() *mongo.Collection {
	return collection(b.model.CollectionName())
}

// SelectById 通过主键查询数据 ObjectId类型 匹配_id字段 当传入的类型是string 将默认让该数据转换为mongo hex 如果string类型非mongo hex 需要将notObjectId设置为true
func (b *BaseMapper[T]) SelectById(id any, result *T, notObjectId ...bool) error {
	queryId, err := b.convertId(id, notObjectId...)
	if err != nil {
		return err
	}
	return CheckSingleResult(collection(b.model.CollectionName()).FindOne(context.Background(), bson.M{"_id": queryId}), result)
}

// SelectByIds 通过主键查询数据 匹配_id字段 当传入的类型是string 将默认让该数据转换为mongo hex 如果string类型非mongo hex 需要将notObjectId设置为true
func (b *BaseMapper[T]) SelectByIds(ids []any, result *[]*T, notObjectId ...bool) (err error) {
	if len(ids) == 0 {
		return errors.New("ids is empty")
	}
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	var queryIDs []any
	if len(notObjectId) > 0 && notObjectId[0] {
		queryIDs = ids
	} else {
		queryIDs = coll.SliceCollect(ids, func(id any) any {
			str, ok := id.(string)
			if !ok {
				return id
			} else {
				hex, err := bson.ObjectIDFromHex(str)
				if err != nil {
					panic(err)
				}
				return hex
			}
		})
	}
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), bson.M{"_id": bson.M{"$in": queryIDs}})
	return CheckMultipleResult(cursor, err, result)
}

// SelectOneByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByCond(condition *T, result *T, specifyColumns ...string) error {
	return CheckSingleResult(collection(b.model.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectOneByBson(condition bson.M, result *T, specifyColumns ...string) error {
	return CheckSingleResult(collection(b.model.CollectionName()).FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByOption 通过原生Collection查询能力
func (b *BaseMapper[T]) SelectOneByOption(filter interface{}, result *T, opts ...options.Lister[options.FindOneOptions]) error {
	return CheckSingleResult(collection(b.model.CollectionName()).FindOne(context.Background(), filter, opts...), result)
}

// SelectByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByCond(condition *T, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), condition, opt)
	return CheckMultipleResult(cursor, err, result)
}

// SelectByBson 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b *BaseMapper[T]) SelectByBson(condition bson.M, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), condition, opt)
	return CheckMultipleResult(cursor, err, result)
}

// SelectByOption 通过原生Collection查询能力
func (b *BaseMapper[T]) SelectByOption(filter interface{}, result *[]*T, opts ...options.Lister[options.FindOptions]) error {
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), filter, opts...)
	return CheckMultipleResult(cursor, err, result)
}

// CountByCond 通过条件查询数据总数
func (b *BaseMapper[T]) CountByCond(condition *T) (int64, error) {
	return collection(b.model.CollectionName()).CountDocuments(context.Background(), condition)
}

// CountByBson 通过条件查询数据总数
func (b *BaseMapper[T]) CountByBson(condition bson.M) (int64, error) {
	return collection(b.model.CollectionName()).CountDocuments(context.Background(), condition)
}

// CountByOption 通过原生Collection查询能力
func (b *BaseMapper[T]) CountByOption(filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return collection(b.model.CollectionName()).CountDocuments(context.Background(), filter, opts...)
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
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), condition, opt)
	return total, CheckMultipleResult(cursor, err, result)
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
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), condition, opt)
	return total, CheckMultipleResult(cursor, err, result)
}

// SelectPageByOption 分页查询 pageNumber >= 1
func (b *BaseMapper[T]) SelectPageByOption(filter interface{}, orderBy []*OrderBy, pageNumber, pageSize int, result *[]*T, opts ...options.Lister[options.FindOptions]) (total int64, err error) {
	if pageNumber <= 0 || pageSize <= 0 {
		return 0, errors.New("pageNumber or pageSize <= 0")
	}
	total, err = b.CountByOption(filter)
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
	cursor, err := collection(b.model.CollectionName()).Find(context.Background(), filter, opts...)
	return total, CheckMultipleResult(cursor, err, result)
}

// Insert 保存数据
func (b *BaseMapper[T]) Insert(entity *T) (string, error) {
	return CheckSingleSaveResult(collection(b.model.CollectionName()).InsertOne(context.Background(), entity))
}

// InsertByBson 保存数据
func (b *BaseMapper[T]) InsertByBson(entity bson.M) (string, error) {
	return CheckSingleSaveResult(collection(b.model.CollectionName()).InsertOne(context.Background(), entity))
}

// InsertWithOption 保存数据 使用Collection原生能力
func (b *BaseMapper[T]) InsertWithOption(document interface{}, opts ...options.Lister[options.InsertOneOptions]) (string, error) {
	return CheckSingleSaveResult(collection(b.model.CollectionName()).InsertOne(context.Background(), document, opts...))
}

// InsertBatch 批量保存数据
func (b *BaseMapper[T]) InsertBatch(entities *[]*T) ([]string, error) {
	return CheckMultipleSaveResult(collection(b.model.CollectionName()).InsertMany(context.Background(), *entities))
}

// InsertBatchByBson 批量保存数据
func (b *BaseMapper[T]) InsertBatchByBson(entities bson.A) ([]string, error) {
	return CheckMultipleSaveResult(collection(b.model.CollectionName()).InsertMany(context.Background(), entities))
}

// InsertBatchByColl 批量保存数据
func (b *BaseMapper[T]) InsertBatchWithOption(documents interface{}, opts ...options.Lister[options.InsertManyOptions]) ([]string, error) {
	return CheckMultipleSaveResult(collection(b.model.CollectionName()).InsertMany(context.Background(), documents, opts...))
}

func (b *BaseMapper[T]) convertId(id any, notObjectId ...bool) (any, error) {
	var queryId any
	if len(notObjectId) > 0 && notObjectId[0] {
		queryId = id
	} else {
		idString, ok := id.(string)
		if ok {
			hex, err := bson.ObjectIDFromHex(idString)
			if err != nil {
				return false, err
			}
			queryId = hex
		} else {
			queryId = id
		}
	}
	return queryId, nil
}

// UpdateById 根据主键更新数据 id ObjectId hex
func (b *BaseMapper[T]) UpdateById(update *T, id any, notObjectId ...bool) (bool, error) {
	queryId, err := b.convertId(id, notObjectId...)
	if err != nil {
		return false, err
	}
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateByID(context.Background(), queryId, bson.M{"$set": update}))
}

// UpdateByIdUseBson 根据主键更新数据
func (b *BaseMapper[T]) UpdateByIdUseBson(update bson.M, id any, notObjectId ...bool) (bool, error) {
	queryId, err := b.convertId(id, notObjectId...)
	if err != nil {
		return false, err
	}
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateByID(context.Background(), queryId, bson.M{"$set": update}))
}

// UpdateOneByCond 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateOneByCond(update, condition *T) (bool, error) {
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateOneByCondUseBson 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateOneByCondUseBson(update, condition bson.M) (bool, error) {
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateByCond 通过条件更新多条数据
func (b *BaseMapper[T]) UpdateByCond(update, condition *T) (bool, error) {
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateByCondUseBson 通过条件更新单条数据
func (b *BaseMapper[T]) UpdateByCondUseBson(update, condition bson.M) (bool, error) {
	return CheckUpdateResult(collection(b.model.CollectionName()).UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// DeleteById 根据主键删除数据
func (b *BaseMapper[T]) DeleteById(id any, notObjectId ...bool) (bool, error) {
	queryId, err := b.convertId(id, notObjectId...)
	if err != nil {
		return false, err
	}
	return CheckDeleteResult(collection(b.model.CollectionName()).DeleteOne(context.Background(), bson.M{"_id": queryId}))
}

// DeleteOneByCond 通过条件删除数据
func (b *BaseMapper[T]) DeleteOneByCond(condition *T) (bool, error) {
	return CheckDeleteResult(collection(b.model.CollectionName()).DeleteOne(context.Background(), condition))
}

// DeleteOneByCondUseBson 通过条件删除数据
func (b *BaseMapper[T]) DeleteOneByCondUseBson(condition bson.M) (bool, error) {
	return CheckDeleteResult(collection(b.model.CollectionName()).DeleteOne(context.Background(), condition))
}

// DeleteByCond 通过条件删除数据
func (b *BaseMapper[T]) DeleteByCond(condition *T) (bool, error) {
	return CheckDeleteResult(collection(b.model.CollectionName()).DeleteMany(context.Background(), condition))
}

// DeleteByCondUseBson 通过条件删除数据
func (b *BaseMapper[T]) DeleteByCondUseBson(condition bson.M) (bool, error) {
	return CheckDeleteResult(collection(b.model.CollectionName()).DeleteMany(context.Background(), condition))
}
