package mongostarter

import (
	"context"
	"reflect"

	"github.com/acexy/golang-toolkit/util/coll"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func collection(name string) (*mongo.Collection, error) {
	result := RawCollection(name)
	if result == nil {
		return nil, ErrMongoStarterNotStarted
	}
	return result, nil
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
		sort := make(bson.D, 0, len(orderBy))
		for _, order := range orderBy {
			value := 1
			if order.Desc {
				value = -1
			}
			sort = append(sort, bson.E{Key: order.Column, Value: value})
		}
		(*opt).SetSort(sort)
	}
}

func setPage(opt **options.FindOptionsBuilder, pageNumber, pageSize int) {
	if *opt == nil {
		*opt = options.Find()
	}
	skip := (pageNumber - 1) * pageSize
	(*opt).SetSkip(int64(skip)).SetLimit(int64(pageSize))
}

func isEmptyCondition(condition any) (bool, error) {
	if condition == nil {
		return true, nil
	}
	value := reflect.ValueOf(condition)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		return true, nil
	}
	data, err := bson.Marshal(condition)
	if err != nil {
		return false, err
	}
	var document bson.D
	if err = bson.Unmarshal(data, &document); err != nil {
		return false, err
	}
	return len(document) == 0, nil
}

// Collection 获取当前 Mapper 对应的原始 Collection。
func (b BaseMapper[T]) Collection() (*mongo.Collection, error) {
	return collection(b.model.CollectionName())
}

// SelectByID 通过主键查询数据，默认将字符串 ID 转换为 ObjectID；普通字符串 ID 需要将 notObjectID 设置为 true
func (b BaseMapper[T]) SelectByID(id any, result *T, notObjectID ...bool) error {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return err
	}
	coll, err := b.Collection()
	if err != nil { return err }
	return checkSingleResult(coll.FindOne(context.Background(), bson.M{"_id": queryID}), result)
}

// SelectByIDs 通过多个主键查询数据，默认将字符串 ID 转换为 ObjectID；普通字符串 ID 需要将 notObjectID 设置为 true
func (b BaseMapper[T]) SelectByIDs(ids []any, result *[]*T, notObjectID ...bool) (err error) {
	if len(ids) == 0 {
		return ErrEmptyIDs
	}
	queryIDs := make([]any, 0, len(ids))
	for _, id := range ids {
		queryID, convertErr := b.convertID(id, notObjectID...)
		if convertErr != nil {
			return convertErr
		}
		queryIDs = append(queryIDs, queryID)
	}
	coll, err := b.Collection()
	if err != nil { return err }
	cursor, err := coll.Find(context.Background(), bson.M{"_id": bson.M{"$in": queryIDs}})
	return checkMultipleResult(cursor, err, result)
}

// SelectOneByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b BaseMapper[T]) SelectOneByCond(condition *T, result *T, specifyColumns ...string) error {
	coll, err := b.Collection()
	if err != nil { return err }
	return checkSingleResult(coll.FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneByBSON 通过 BSON 条件查询一条数据
// specifyColumns 需要指定只查询的数据库字段
func (b BaseMapper[T]) SelectOneByBSON(condition bson.M, result *T, specifyColumns ...string) error {
	coll, err := b.Collection()
	if err != nil { return err }
	return checkSingleResult(coll.FindOne(context.Background(), condition, specifyColumnsOneOpt(specifyColumns...)), result)
}

// SelectOneWithOptions 使用原生 FindOneOptions 查询一条数据
func (b BaseMapper[T]) SelectOneWithOptions(filter any, result *T, opts ...options.Lister[options.FindOneOptions]) error {
	coll, err := b.Collection()
	if err != nil { return err }
	return checkSingleResult(coll.FindOne(context.Background(), filter, opts...), result)
}

// SelectByCond 通过条件查询
// specifyColumns 需要指定只查询的数据库字段
func (b BaseMapper[T]) SelectByCond(condition *T, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	coll, err := b.Collection()
	if err != nil { return err }
	cursor, err := coll.Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectByBSON 通过 BSON 条件查询数据
// specifyColumns 需要指定只查询的数据库字段
func (b BaseMapper[T]) SelectByBSON(condition bson.M, orderBy []*OrderBy, result *[]*T, specifyColumns ...string) error {
	opt := specifyColumnsOpt(specifyColumns...)
	if len(orderBy) > 0 {
		setOrderBy(&opt, orderBy)
	}
	coll, err := b.Collection()
	if err != nil { return err }
	cursor, err := coll.Find(context.Background(), condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectWithOptions 使用原生 FindOptions 查询数据
func (b BaseMapper[T]) SelectWithOptions(filter any, result *[]*T, opts ...options.Lister[options.FindOptions]) error {
	coll, err := b.Collection()
	if err != nil { return err }
	cursor, err := coll.Find(context.Background(), filter, opts...)
	return checkMultipleResult(cursor, err, result)
}

// CountByCond 通过条件查询数据总数
func (b BaseMapper[T]) CountByCond(condition *T) (int64, error) {
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return coll.CountDocuments(context.Background(), condition)
}

// CountByBSON 通过 BSON 条件统计数据总数
func (b BaseMapper[T]) CountByBSON(condition bson.M) (int64, error) {
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return coll.CountDocuments(context.Background(), condition)
}

// CountWithOptions 使用原生 CountOptions 统计数据总数
func (b BaseMapper[T]) CountWithOptions(filter any, opts ...options.Lister[options.CountOptions]) (int64, error) {
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return coll.CountDocuments(context.Background(), filter, opts...)
}

// SelectPageByCond 通过实体条件分页查询
func (b BaseMapper[T]) SelectPageByCond(condition *T, query PageQuery, result *[]*T) (total int64, err error) {
	if query.PageNumber <= 0 || query.PageSize <= 0 {
		return 0, ErrInvalidPage
	}
	total, err = b.CountByCond(condition)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(query.SpecifyColumns...)
	if len(query.OrderBy) > 0 {
		setOrderBy(&opt, query.OrderBy)
	}
	setPage(&opt, query.PageNumber, query.PageSize)
	coll, err := b.Collection()
	if err != nil { return 0, err }
	cursor, err := coll.Find(context.Background(), condition, opt)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageByBSON 通过 BSON 条件分页查询
func (b BaseMapper[T]) SelectPageByBSON(condition bson.M, query PageQuery, result *[]*T) (total int64, err error) {
	if query.PageNumber <= 0 || query.PageSize <= 0 {
		return 0, ErrInvalidPage
	}
	total, err = b.CountByBSON(condition)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(query.SpecifyColumns...)
	if len(query.OrderBy) > 0 {
		setOrderBy(&opt, query.OrderBy)
	}
	setPage(&opt, query.PageNumber, query.PageSize)
	coll, err := b.Collection()
	if err != nil { return 0, err }
	cursor, err := coll.Find(context.Background(), condition, opt)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageWithOptions 使用原生查询选项分页查询
func (b BaseMapper[T]) SelectPageWithOptions(filter any, query PageQuery, result *[]*T) (total int64, err error) {
	if query.PageNumber <= 0 || query.PageSize <= 0 {
		return 0, ErrInvalidPage
	}
	total, err = b.CountWithOptions(filter, query.CountOptions...)
	if err != nil {
		return 0, err
	}

	if len(query.OrderBy) > 0 {
		sort := make(bson.D, 0, len(query.OrderBy))
		for _, order := range query.OrderBy {
			value := 1
			if order.Desc {
				value = -1
			}
			sort = append(sort, bson.E{Key: order.Column, Value: value})
		}
		query.FindOptions = append(query.FindOptions, options.Find().SetSort(sort))
	}
	skip := (query.PageNumber - 1) * query.PageSize
	query.FindOptions = append(query.FindOptions, options.Find().SetSkip(int64(skip)).SetLimit(int64(query.PageSize)))
	coll, err := b.Collection()
	if err != nil { return 0, err }
	cursor, err := coll.Find(context.Background(), filter, query.FindOptions...)
	return total, checkMultipleResult(cursor, err, result)
}

// Insert 保存数据
func (b BaseMapper[T]) Insert(entity *T) (string, error) {
	coll, err := b.Collection()
	if err != nil { return "", err }
	return checkSingleInsertResult(coll.InsertOne(context.Background(), entity))
}

// InsertWithBSON 使用 BSON 文档插入数据
func (b BaseMapper[T]) InsertWithBSON(entity bson.M) (string, error) {
	coll, err := b.Collection()
	if err != nil { return "", err }
	return checkSingleInsertResult(coll.InsertOne(context.Background(), entity))
}

// InsertWithOptions 使用原生 InsertOneOptions 插入数据
func (b BaseMapper[T]) InsertWithOptions(document any, opts ...options.Lister[options.InsertOneOptions]) (string, error) {
	coll, err := b.Collection()
	if err != nil { return "", err }
	return checkSingleInsertResult(coll.InsertOne(context.Background(), document, opts...))
}

// InsertBatch 批量保存数据
func (b BaseMapper[T]) InsertBatch(entities []*T) ([]string, error) {
	coll, err := b.Collection()
	if err != nil { return nil, err }
	return checkMultipleInsertResult(coll.InsertMany(context.Background(), entities))
}

// InsertBatchWithBSON 使用 BSON 文档批量插入数据
func (b BaseMapper[T]) InsertBatchWithBSON(entities bson.A) ([]string, error) {
	coll, err := b.Collection()
	if err != nil { return nil, err }
	return checkMultipleInsertResult(coll.InsertMany(context.Background(), entities))
}

// InsertBatchWithOptions 使用原生 InsertManyOptions 批量插入数据
func (b BaseMapper[T]) InsertBatchWithOptions(documents any, opts ...options.Lister[options.InsertManyOptions]) ([]string, error) {
	coll, err := b.Collection()
	if err != nil { return nil, err }
	return checkMultipleInsertResult(coll.InsertMany(context.Background(), documents, opts...))
}

func (b BaseMapper[T]) convertID(id any, notObjectID ...bool) (any, error) {
	var queryID any
	if len(notObjectID) > 0 && notObjectID[0] {
		queryID = id
	} else {
		idString, ok := id.(string)
		if ok {
			hex, err := bson.ObjectIDFromHex(idString)
			if err != nil {
				return nil, err
			}
			queryID = hex
		} else {
			queryID = id
		}
	}
	return queryID, nil
}

// UpdateByID 根据主键更新数据
func (b BaseMapper[T]) UpdateByID(update *T, id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateByID(context.Background(), queryID, bson.M{"$set": update}))
}

// UpdateByIDWithBSON 根据主键使用 BSON 文档更新数据
func (b BaseMapper[T]) UpdateByIDWithBSON(update bson.M, id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateByID(context.Background(), queryID, bson.M{"$set": update}))
}

// UpdateOneByCond 通过条件更新单条数据
func (b BaseMapper[T]) UpdateOneByCond(update, condition *T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateOneByBSON 通过 BSON 条件更新一条数据
func (b BaseMapper[T]) UpdateOneByBSON(update, condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateByCond 通过条件更新多条数据
func (b BaseMapper[T]) UpdateByCond(update, condition *T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateByBSON 通过 BSON 条件更新多条数据
func (b BaseMapper[T]) UpdateByBSON(update, condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkUpdateResult(coll.UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// DeleteByID 根据主键删除数据
func (b BaseMapper[T]) DeleteByID(id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkDeleteResult(coll.DeleteOne(context.Background(), bson.M{"_id": queryID}))
}

// DeleteOneByCond 通过条件删除数据
func (b BaseMapper[T]) DeleteOneByCond(condition *T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkDeleteResult(coll.DeleteOne(context.Background(), condition))
}

// DeleteOneByBSON 通过 BSON 条件删除一条数据
func (b BaseMapper[T]) DeleteOneByBSON(condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkDeleteResult(coll.DeleteOne(context.Background(), condition))
}

// DeleteByCond 通过条件删除数据
func (b BaseMapper[T]) DeleteByCond(condition *T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkDeleteResult(coll.DeleteMany(context.Background(), condition))
}

// DeleteByBSON 通过 BSON 条件删除多条数据
func (b BaseMapper[T]) DeleteByBSON(condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := b.Collection()
	if err != nil { return 0, err }
	return checkDeleteResult(coll.DeleteMany(context.Background(), condition))
}
