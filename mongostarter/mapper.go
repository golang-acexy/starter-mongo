package mongostarter

import (
	"context"
	"math"
	"reflect"
	"strings"

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
		column := coll.SliceFilterToMap(specifyColumns, func(column string) (string, int, bool) {
			return column, 1, true
		})
		if !coll.SliceContains(specifyColumns, "_id") {
			column["_id"] = 0
		}
		return options.FindOne().SetProjection(column)
	}
	return nil
}

func specifyColumnsOpt(specifyColumns ...string) *options.FindOptionsBuilder {
	if len(specifyColumns) > 0 {
		column := coll.SliceFilterToMap(specifyColumns, func(column string) (string, int, bool) {
			return column, 1, true
		})
		if !coll.SliceContains(specifyColumns, "_id") {
			column["_id"] = 0
		}
		return options.Find().SetProjection(column)
	}
	return nil
}
func buildSort(orderBy []*OrderBy) (bson.D, error) {
	sort := make(bson.D, 0, len(orderBy))
	for _, order := range orderBy {
		if order == nil || strings.TrimSpace(order.Column) == "" {
			return nil, ErrInvalidOrderBy
		}
		value := 1
		if order.Desc {
			value = -1
		}
		sort = append(sort, bson.E{Key: order.Column, Value: value})
	}
	return sort, nil
}

func validateOrderBy(orderBy []*OrderBy) error {
	_, err := buildSort(orderBy)
	return err
}

func setOrderBy(opt **options.FindOptionsBuilder, orderBy []*OrderBy) error {
	if *opt == nil {
		*opt = options.Find()
	}
	if len(orderBy) > 0 {
		sort, err := buildSort(orderBy)
		if err != nil {
			return err
		}
		(*opt).SetSort(sort)
	}
	return nil
}

func setOneOrderBy(opt **options.FindOneOptionsBuilder, orderBy []*OrderBy) error {
	if *opt == nil {
		*opt = options.FindOne()
	}
	if len(orderBy) > 0 {
		sort, err := buildSort(orderBy)
		if err != nil {
			return err
		}
		(*opt).SetSort(sort)
	}
	return nil
}

func pageOffset(number, size int) (int64, error) {
	if number <= 0 || size <= 0 {
		return 0, ErrInvalidPage
	}
	number64, size64 := int64(number), int64(size)
	if number64-1 > math.MaxInt64/size64 {
		return 0, ErrInvalidPage
	}
	return (number64 - 1) * size64, nil
}

func setPage(opt **options.FindOptionsBuilder, number, size int) error {
	skip, err := pageOffset(number, size)
	if err != nil {
		return err
	}
	if *opt == nil {
		*opt = options.Find()
	}
	(*opt).SetSkip(skip).SetLimit(int64(size))
	return nil
}

func setLimit(opt **options.FindOptionsBuilder, limit int) {
	if *opt == nil {
		*opt = options.Find()
	}
	(*opt).SetLimit(int64(limit))
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

func normalizeBSONUpdate(update bson.M) bson.M {
	for key := range update {
		if strings.HasPrefix(key, "$") {
			return update
		}
	}
	return bson.M{"$set": update}
}

// Collection returns the borrowed model collection, or nil outside the active lifecycle.
func (b BaseMapper[T]) Collection() *mongo.Collection {
	return RawCollection(b.model.CollectionName())
}

// SelectByID decodes one document into a nonnil destination; absence returns mongo.ErrNoDocuments.
// Strings become ObjectIDs unless the first notObjectID value is true; extra values are ignored.
func (b BaseMapper[T]) SelectByID(id any, result *T, notObjectID ...bool) error {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	return checkSingleResult(coll.FindOne(context.Background(), bson.M{"_id": queryID}), result)
}

// SelectByIDs decodes matching documents into a nonnil slice pointer; empty IDs return ErrEmptyIDs.
// Strings become ObjectIDs unless the first notObjectID value is true. Results do not preserve input ID order.
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
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	cursor, err := coll.Find(context.Background(), bson.M{"_id": bson.M{"$in": queryIDs}})
	return checkMultipleResult(cursor, err, result)
}

// ExistsByID counts matching documents and propagates driver errors.
// Strings become ObjectIDs unless the first notObjectID value is true.
func (b BaseMapper[T]) ExistsByID(id any, notObjectID ...bool) (bool, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return false, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return false, err
	}
	count, err := coll.CountDocuments(context.Background(), bson.M{"_id": queryID})
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// SelectOneByCond decodes one typed-filter match using projection and sorting; Limit is ignored.
func (b BaseMapper[T]) SelectOneByCond(query CondQuery[T], result *T) error {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	opt := specifyColumnsOneOpt(query.SelectColumns...)
	if err = setOneOrderBy(&opt, query.OrderBy); err != nil {
		return err
	}
	return checkSingleResult(coll.FindOne(context.Background(), query.Condition, opt), result)
}

// SelectOneByBSON decodes one BSON-filter match using projection and sorting; Limit is ignored.
func (b BaseMapper[T]) SelectOneByBSON(query BSONQuery, result *T) error {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	opt := specifyColumnsOneOpt(query.SelectColumns...)
	if err = setOneOrderBy(&opt, query.OrderBy); err != nil {
		return err
	}
	return checkSingleResult(coll.FindOne(context.Background(), query.Condition, opt), result)
}

// SelectOneWithOptions decodes one match using driver options; absence returns mongo.ErrNoDocuments.
func (b BaseMapper[T]) SelectOneWithOptions(filter any, result *T, opts ...options.Lister[options.FindOneOptions]) error {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	return checkSingleResult(coll.FindOne(context.Background(), filter, opts...), result)
}

// SelectByCond decodes typed-filter matches; zero Limit is unlimited and a negative Limit is rejected.
func (b BaseMapper[T]) SelectByCond(query CondQuery[T], result *[]*T) error {
	if query.Limit < 0 {
		return ErrInvalidQueryRange
	}
	opt := specifyColumnsOpt(query.SelectColumns...)
	if len(query.OrderBy) > 0 {
		if err := setOrderBy(&opt, query.OrderBy); err != nil {
			return err
		}
	}
	if query.Limit > 0 {
		setLimit(&opt, query.Limit)
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	cursor, err := coll.Find(context.Background(), query.Condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectByBSON decodes BSON-filter matches; zero Limit is unlimited and a negative Limit is rejected.
func (b BaseMapper[T]) SelectByBSON(query BSONQuery, result *[]*T) error {
	if query.Limit < 0 {
		return ErrInvalidQueryRange
	}
	opt := specifyColumnsOpt(query.SelectColumns...)
	if len(query.OrderBy) > 0 {
		if err := setOrderBy(&opt, query.OrderBy); err != nil {
			return err
		}
	}
	if query.Limit > 0 {
		setLimit(&opt, query.Limit)
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	cursor, err := coll.Find(context.Background(), query.Condition, opt)
	return checkMultipleResult(cursor, err, result)
}

// SelectWithOptions decodes matching documents into a nonnil slice pointer using driver options.
func (b BaseMapper[T]) SelectWithOptions(filter any, result *[]*T, opts ...options.Lister[options.FindOptions]) error {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return err
	}
	cursor, err := coll.Find(context.Background(), filter, opts...)
	return checkMultipleResult(cursor, err, result)
}

// CountByCond counts typed-filter matches; projection, sorting, and Limit are ignored.
func (b BaseMapper[T]) CountByCond(query CondQuery[T]) (int64, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return coll.CountDocuments(context.Background(), query.Condition)
}

// CountByBSON counts BSON-filter matches; projection, sorting, and Limit are ignored.
func (b BaseMapper[T]) CountByBSON(query BSONQuery) (int64, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return coll.CountDocuments(context.Background(), query.Condition)
}

// CountWithOptions counts matching documents using driver options, which may restrict the total.
func (b BaseMapper[T]) CountWithOptions(filter any, opts ...options.Lister[options.CountOptions]) (int64, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return coll.CountDocuments(context.Background(), filter, opts...)
}

// SelectPageByCond counts then decodes a typed-filter page without a shared snapshot.
// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
func (b BaseMapper[T]) SelectPageByCond(query PageQuery[T], result *[]*T) (total int64, err error) {
	if _, err = pageOffset(query.Number, query.Size); err != nil {
		return 0, err
	}
	if err = validateOrderBy(query.OrderBy); err != nil {
		return 0, err
	}
	total, err = b.CountWithOptions(query.Condition, query.CountOptions...)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(query.SelectColumns...)
	if len(query.OrderBy) > 0 {
		if err = setOrderBy(&opt, query.OrderBy); err != nil {
			return 0, err
		}
	}
	if err = setPage(&opt, query.Number, query.Size); err != nil {
		return 0, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	query.FindOptions = append(query.FindOptions, opt)
	cursor, err := coll.Find(context.Background(), query.Condition, query.FindOptions...)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageByBSON counts then decodes a BSON-filter page without a shared snapshot.
// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
func (b BaseMapper[T]) SelectPageByBSON(query BSONPageQuery, result *[]*T) (total int64, err error) {
	if _, err = pageOffset(query.Number, query.Size); err != nil {
		return 0, err
	}
	if err = validateOrderBy(query.OrderBy); err != nil {
		return 0, err
	}
	total, err = b.CountWithOptions(query.Condition, query.CountOptions...)
	if err != nil {
		return 0, err
	}
	opt := specifyColumnsOpt(query.SelectColumns...)
	if len(query.OrderBy) > 0 {
		if err = setOrderBy(&opt, query.OrderBy); err != nil {
			return 0, err
		}
	}
	if err = setPage(&opt, query.Number, query.Size); err != nil {
		return 0, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	query.FindOptions = append(query.FindOptions, opt)
	cursor, err := coll.Find(context.Background(), query.Condition, query.FindOptions...)
	return total, checkMultipleResult(cursor, err, result)
}

// SelectPageWithOptions counts then decodes a native-filter page without a shared snapshot.
// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
func (b BaseMapper[T]) SelectPageWithOptions(query FilterPageQuery, result *[]*T) (total int64, err error) {
	if _, err = pageOffset(query.Number, query.Size); err != nil {
		return 0, err
	}
	if err = validateOrderBy(query.OrderBy); err != nil {
		return 0, err
	}
	total, err = b.CountWithOptions(query.Filter, query.CountOptions...)
	if err != nil {
		return 0, err
	}

	if len(query.SelectColumns) > 0 {
		query.FindOptions = append(query.FindOptions, specifyColumnsOpt(query.SelectColumns...))
	}
	if len(query.OrderBy) > 0 {
		sort, sortErr := buildSort(query.OrderBy)
		if sortErr != nil {
			return 0, sortErr
		}
		query.FindOptions = append(query.FindOptions, options.Find().SetSort(sort))
	}
	skip, pageErr := pageOffset(query.Number, query.Size)
	if pageErr != nil {
		return 0, pageErr
	}
	query.FindOptions = append(query.FindOptions, options.Find().SetSkip(skip).SetLimit(int64(query.Size)))
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	cursor, err := coll.Find(context.Background(), query.Filter, query.FindOptions...)
	return total, checkMultipleResult(cursor, err, result)
}

// Insert writes a document and returns its acknowledged ID; ObjectIDs are formatted as hexadecimal strings.
func (b BaseMapper[T]) Insert(entity *T) (string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return "", err
	}
	return checkSingleInsertResult(coll.InsertOne(context.Background(), entity))
}

// InsertWithBSON writes a BSON document and returns its acknowledged ID as a string.
func (b BaseMapper[T]) InsertWithBSON(entity bson.M) (string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return "", err
	}
	return checkSingleInsertResult(coll.InsertOne(context.Background(), entity))
}

// InsertWithOptions writes a native document using driver options and returns its acknowledged ID.
func (b BaseMapper[T]) InsertWithOptions(document any, opts ...options.Lister[options.InsertOneOptions]) (string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return "", err
	}
	return checkSingleInsertResult(coll.InsertOne(context.Background(), document, opts...))
}

// InsertBatch writes documents and returns acknowledged IDs as strings.
// A driver error may follow partial insertion; this helper returns no IDs on error.
func (b BaseMapper[T]) InsertBatch(entities []*T) ([]string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return nil, err
	}
	return checkMultipleInsertResult(coll.InsertMany(context.Background(), entities))
}

// InsertBatchWithBSON writes BSON documents and returns acknowledged IDs as strings.
// A driver error may follow partial insertion; this helper returns no IDs on error.
func (b BaseMapper[T]) InsertBatchWithBSON(entities bson.A) ([]string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return nil, err
	}
	return checkMultipleInsertResult(coll.InsertMany(context.Background(), entities))
}

// InsertBatchWithOptions writes native documents using driver options and returns acknowledged IDs.
// A driver error may follow partial insertion; this helper returns no IDs on error.
func (b BaseMapper[T]) InsertBatchWithOptions(documents any, opts ...options.Lister[options.InsertManyOptions]) ([]string, error) {
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return nil, err
	}
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

// UpdateByID applies a typed $set update and returns the modified count, not the matched count.
// Strings become ObjectIDs unless the first notObjectID value is true.
func (b BaseMapper[T]) UpdateByID(update *T, id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateByID(context.Background(), queryID, bson.M{"$set": update}))
}

// UpdateByIDWithBSON applies BSON operators or wraps plain fields in $set; returns the modified count.
// Strings become ObjectIDs unless the first notObjectID value is true.
func (b BaseMapper[T]) UpdateByIDWithBSON(update bson.M, id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateByID(context.Background(), queryID, normalizeBSONUpdate(update)))
}

// UpdateOneByCond applies a typed $set update to one match and returns its modified count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) UpdateOneByCond(update *T, condition T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateOne(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateOneByBSON rejects an empty map filter, applies BSON operators/plain fields, and returns the modified count.
func (b BaseMapper[T]) UpdateOneByBSON(update, condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateOne(context.Background(), condition, normalizeBSONUpdate(update)))
}

// UpdateByCond applies a typed $set update to all matches and returns the modified count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) UpdateByCond(update *T, condition T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateMany(context.Background(), condition, bson.M{"$set": update}))
}

// UpdateByBSON rejects an empty map filter, applies BSON operators/plain fields, and returns the modified count.
func (b BaseMapper[T]) UpdateByBSON(update, condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateMany(context.Background(), condition, normalizeBSONUpdate(update)))
}

// UpdateOneWithOptions updates one match using native update data/options and returns the modified count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) UpdateOneWithOptions(filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (int64, error) {
	empty, err := isEmptyCondition(filter)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateOne(context.Background(), filter, update, opts...))
}

// UpdateWithOptions updates all matches using native update data/options and returns the modified count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) UpdateWithOptions(filter, update any, opts ...options.Lister[options.UpdateManyOptions]) (int64, error) {
	empty, err := isEmptyCondition(filter)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkUpdateResult(coll.UpdateMany(context.Background(), filter, update, opts...))
}

// DeleteByID removes one ID match and returns its deleted count.
// Strings become ObjectIDs unless the first notObjectID value is true.
func (b BaseMapper[T]) DeleteByID(id any, notObjectID ...bool) (int64, error) {
	queryID, err := b.convertID(id, notObjectID...)
	if err != nil {
		return 0, err
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteOne(context.Background(), bson.M{"_id": queryID}))
}

// DeleteByIDs removes ID matches and returns the deleted count; empty IDs return ErrEmptyIDs.
// Strings become ObjectIDs unless the first notObjectID value is true.
func (b BaseMapper[T]) DeleteByIDs(ids []any, notObjectID ...bool) (int64, error) {
	if len(ids) == 0 {
		return 0, ErrEmptyIDs
	}
	queryIDs := make([]any, 0, len(ids))
	for _, id := range ids {
		queryID, err := b.convertID(id, notObjectID...)
		if err != nil {
			return 0, err
		}
		queryIDs = append(queryIDs, queryID)
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteMany(context.Background(), bson.M{"_id": bson.M{"$in": queryIDs}}))
}

// DeleteOneByCond deletes one typed-filter match and returns the deleted count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) DeleteOneByCond(condition T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteOne(context.Background(), condition))
}

// DeleteOneByBSON rejects an empty map filter and returns the deleted count for one match.
func (b BaseMapper[T]) DeleteOneByBSON(condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteOne(context.Background(), condition))
}

// DeleteByCond deletes all typed-filter matches and returns the deleted count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) DeleteByCond(condition T) (int64, error) {
	empty, err := isEmptyCondition(condition)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteMany(context.Background(), condition))
}

// DeleteByBSON rejects an empty map filter and returns the deleted count for all matches.
func (b BaseMapper[T]) DeleteByBSON(condition bson.M) (int64, error) {
	if len(condition) == 0 {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteMany(context.Background(), condition))
}

// DeleteOneWithOptions deletes one native-filter match using driver options and returns the deleted count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) DeleteOneWithOptions(filter any, opts ...options.Lister[options.DeleteOneOptions]) (int64, error) {
	empty, err := isEmptyCondition(filter)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteOne(context.Background(), filter, opts...))
}

// DeleteWithOptions deletes all native-filter matches using driver options and returns the deleted count.
// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
func (b BaseMapper[T]) DeleteWithOptions(filter any, opts ...options.Lister[options.DeleteManyOptions]) (int64, error) {
	empty, err := isEmptyCondition(filter)
	if err != nil {
		return 0, err
	}
	if empty {
		return 0, ErrEmptyCondition
	}
	coll, err := collection(b.model.CollectionName())
	if err != nil {
		return 0, err
	}
	return checkDeleteResult(coll.DeleteMany(context.Background(), filter, opts...))
}
