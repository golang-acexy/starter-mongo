package mongostarter

import (
	"time"

	"github.com/acexy/golang-toolkit/util/coll"
	"github.com/acexy/golang-toolkit/util/json"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Model supplies the collection name for the zero value of T; implementations must handle that value.
type Model interface {
	// CollectionName returns a stable collection name and must work on the model zero value.
	CollectionName() string
}

// BaseMapper provides driver-facing CRUD with caller-owned result destinations and background contexts.
// Use a model whose zero value supports CollectionName. Driver handles support concurrent operations,
// but result buffers, filters, and mutable option inputs must not be shared without synchronization.
// No HTTP cancellation or session context is inherited; use raw collections for those operations.
type BaseMapper[T Model] struct {
	model T
}

// OrderBy describes one BSON field sort; slice order determines sort precedence.
type OrderBy struct {
	// Column is the BSON field name; a blank name is rejected during query execution.
	Column string
	// Desc selects descending order; false selects ascending order.
	Desc bool
}

// QueryOptions configures projection, ordered sorting, and list-only limits; keep inputs stable during use.
type QueryOptions struct {
	// OrderBy lists sort rules in precedence order; nil entries and blank columns are rejected.
	OrderBy       []*OrderBy
	// SelectColumns specifies inclusion fields; _id is excluded unless explicitly selected.
	SelectColumns []string
	// Limit applies only to list reads: zero is unlimited, negative values are rejected.
	Limit         int
}

// PageOptions contains all pagination inputs. Native count and find options are applied separately.
// Page methods append to FindOptions and may reuse its backing storage; concurrent reuse is unsupported.
type PageOptions struct {
	// Number is the one-based page number; nonpositive values are rejected.
	Number        int
	// Size is the positive page limit; no application-wide maximum is enforced.
	Size          int
	// OrderBy lists sort rules in precedence order; nil entries and blank columns are rejected.
	OrderBy       []*OrderBy
	// SelectColumns specifies inclusion fields; _id is excluded unless explicitly selected.
	SelectColumns []string
	// FindOptions supplies native find options; explicit page settings take precedence.
	FindOptions   []options.Lister[options.FindOptions]
	// CountOptions supplies native count options, which can affect totals independently of the page.
	CountOptions  []options.Lister[options.CountOptions]
}

// PageQuery combines a typed filter with pagination; model BSON tags determine filter fields.
type PageQuery[T Model] struct {
	// Condition supplies the filter; maps and reference-bearing values are borrowed.
	Condition T
	// PageOptions supplies pagination, projection, sorting, and native options.
	PageOptions
}

// BSONPageQuery combines a borrowed BSON map filter with pagination.
type BSONPageQuery struct {
	// Condition supplies the filter; maps and reference-bearing values are borrowed.
	Condition bson.M
	// PageOptions supplies pagination, projection, sorting, and native options.
	PageOptions
}

// FilterPageQuery combines a borrowed driver-native filter with pagination.
type FilterPageQuery struct {
	// Filter supplies the borrowed driver-native filter.
	Filter any
	// PageOptions supplies pagination, projection, sorting, and native options.
	PageOptions
}

// CondQuery combines a typed filter with projection, sorting, and list limits.
type CondQuery[T Model] struct {
	// Condition supplies the filter; maps and reference-bearing values are borrowed.
	Condition T
	// QueryOptions supplies projection, sorting, and list limits.
	QueryOptions
}

// BSONQuery combines a borrowed BSON map filter with projection, sorting, and list limits.
type BSONQuery struct {
	// Condition supplies the filter; maps and reference-bearing values are borrowed.
	Condition bson.M
	// QueryOptions supplies projection, sorting, and list limits.
	QueryOptions
}

// NewOrderBy creates one sort rule; field validation occurs when the query is executed.
func NewOrderBy(column string, desc bool) []*OrderBy {
	return []*OrderBy{{Column: column, Desc: desc}}
}

// NewOrderBys copies sort rules in input order; empty input produces no sorting.
func NewOrderBys(orderBy ...OrderBy) []*OrderBy {
	return coll.SliceCollect(orderBy, func(order OrderBy) *OrderBy {
		result := order
		return &result
	})
}

// Timestamp stores BSON dates at millisecond precision; a zero time encodes as BSON null.
// JSON units follow the toolkit global timestamp setting. Synchronize access when mutating shared values.
type Timestamp json.Timestamp

// NewTimestamp wraps a time value without changing its location or precision.
func NewTimestamp(time time.Time) Timestamp {
	return Timestamp{Time: time}
}

// MarshalBSONValue encodes a BSON date at millisecond precision, or BSON null for zero time.
func (t Timestamp) MarshalBSONValue() (typ byte, data []byte, err error) {
	if t.IsZero() {
		return byte(bson.TypeNull), nil, nil
	}
	valueType, data, err := bson.MarshalValue(bson.DateTime(t.Time.UnixMilli()))
	typ = byte(valueType)
	return
}

// UnmarshalBSONValue decodes into a nonnil receiver; BSON null resets it to zero and errors preserve it.
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

// MarshalJSON uses toolkit timestamp settings; zero time becomes numeric zero.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Time2Timestamp(t.Time)
}

// UnmarshalJSON uses toolkit timestamp settings; JSON null resets a nonnil receiver to zero.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	formatTime, err := json.Timestamp2Time(data)
	if err != nil {
		return err
	}
	t.Time = formatTime
	return nil
}

// RawMapper exposes the shared collection without transferring client ownership.
type RawMapper interface {
	// Collection returns the borrowed model collection, or nil outside the active lifecycle.
	Collection() *mongo.Collection
}

// QueryMapper queries into caller-owned destinations using background contexts; native options extend queries.
type QueryMapper[T Model] interface {
	// SelectByID decodes one document into a nonnil destination; absence returns mongo.ErrNoDocuments.
	// Strings become ObjectIDs unless the first notObjectID value is true; extra values are ignored.
	SelectByID(id any, result *T, notObjectID ...bool) error

	// SelectByIDs decodes matching documents into a nonnil slice pointer; empty IDs return ErrEmptyIDs.
	// Strings become ObjectIDs unless the first notObjectID value is true. Results do not preserve input ID order.
	SelectByIDs(ids []any, result *[]*T, notObjectID ...bool) (err error)

	// ExistsByID counts matching documents and propagates driver errors.
	// Strings become ObjectIDs unless the first notObjectID value is true.
	ExistsByID(id any, notObjectID ...bool) (bool, error)

	// SelectOneByCond decodes one typed-filter match using projection and sorting; Limit is ignored.
	SelectOneByCond(query CondQuery[T], result *T) error

	// SelectOneByBSON decodes one BSON-filter match using projection and sorting; Limit is ignored.
	SelectOneByBSON(query BSONQuery, result *T) error

	// SelectOneWithOptions decodes one match using driver options; absence returns mongo.ErrNoDocuments.
	SelectOneWithOptions(filter any, result *T, opts ...options.Lister[options.FindOneOptions]) error

	// SelectByCond decodes typed-filter matches; zero Limit is unlimited and a negative Limit is rejected.
	SelectByCond(query CondQuery[T], result *[]*T) error

	// SelectByBSON decodes BSON-filter matches; zero Limit is unlimited and a negative Limit is rejected.
	SelectByBSON(query BSONQuery, result *[]*T) error

	// SelectWithOptions decodes matching documents into a nonnil slice pointer using driver options.
	SelectWithOptions(filter any, result *[]*T, opts ...options.Lister[options.FindOptions]) error

	// CountByCond counts typed-filter matches; projection, sorting, and Limit are ignored.
	CountByCond(query CondQuery[T]) (int64, error)

	// CountByBSON counts BSON-filter matches; projection, sorting, and Limit are ignored.
	CountByBSON(query BSONQuery) (int64, error)

	// CountWithOptions counts matching documents using driver options, which may restrict the total.
	CountWithOptions(filter any, opts ...options.Lister[options.CountOptions]) (int64, error)

	// SelectPageByCond counts then decodes a typed-filter page without a shared snapshot.
	// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
	SelectPageByCond(query PageQuery[T], result *[]*T) (total int64, err error)

	// SelectPageByBSON counts then decodes a BSON-filter page without a shared snapshot.
	// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
	SelectPageByBSON(query BSONPageQuery, result *[]*T) (total int64, err error)

	// SelectPageWithOptions counts then decodes a native-filter page without a shared snapshot.
	// Positive Number/Size are required; page options override native pagination. Do not share FindOptions backing storage concurrently.
	SelectPageWithOptions(query FilterPageQuery, result *[]*T) (total int64, err error)
}

// InsertMapper returns acknowledged IDs; failed batches can have partial database effects.
type InsertMapper[T Model] interface {
	// Insert writes a document and returns its acknowledged ID; ObjectIDs are formatted as hexadecimal strings.
	Insert(entity *T) (string, error)

	// InsertWithBSON writes a BSON document and returns its acknowledged ID as a string.
	InsertWithBSON(entity bson.M) (string, error)

	// InsertWithOptions writes a native document using driver options and returns its acknowledged ID.
	InsertWithOptions(document any, opts ...options.Lister[options.InsertOneOptions]) (string, error)

	// InsertBatch writes documents and returns acknowledged IDs as strings.
	// A driver error may follow partial insertion; this helper returns no IDs on error.
	InsertBatch(entities []*T) ([]string, error)

	// InsertBatchWithBSON writes BSON documents and returns acknowledged IDs as strings.
	// A driver error may follow partial insertion; this helper returns no IDs on error.
	InsertBatchWithBSON(entities bson.A) ([]string, error)

	// InsertBatchWithOptions writes native documents using driver options and returns acknowledged IDs.
	// A driver error may follow partial insertion; this helper returns no IDs on error.
	InsertBatchWithOptions(documents any, opts ...options.Lister[options.InsertManyOptions]) ([]string, error)
}

// UpdateMapper returns modified counts; matching an unchanged document can return zero.
// Default-encoding filter guards do not guarantee selectivity under custom BSON options.
type UpdateMapper[T Model] interface {
	// UpdateByID applies a typed $set update and returns the modified count, not the matched count.
	// Strings become ObjectIDs unless the first notObjectID value is true.
	UpdateByID(update *T, id any, notObjectID ...bool) (int64, error)

	// UpdateByIDWithBSON applies BSON operators or wraps plain fields in $set; returns the modified count.
	// Strings become ObjectIDs unless the first notObjectID value is true.
	UpdateByIDWithBSON(update bson.M, id any, notObjectID ...bool) (int64, error)

	// UpdateOneByCond applies a typed $set update to one match and returns its modified count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	UpdateOneByCond(update *T, condition T) (int64, error)

	// UpdateOneByBSON rejects an empty map filter, applies BSON operators/plain fields, and returns the modified count.
	UpdateOneByBSON(update, condition bson.M) (int64, error)

	// UpdateByCond applies a typed $set update to all matches and returns the modified count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	UpdateByCond(update *T, condition T) (int64, error)

	// UpdateByBSON rejects an empty map filter, applies BSON operators/plain fields, and returns the modified count.
	UpdateByBSON(update, condition bson.M) (int64, error)

	// UpdateOneWithOptions updates one match using native update data/options and returns the modified count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	UpdateOneWithOptions(filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (int64, error)

	// UpdateWithOptions updates all matches using native update data/options and returns the modified count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	UpdateWithOptions(filter, update any, opts ...options.Lister[options.UpdateManyOptions]) (int64, error)
}

// DeleteMapper returns deleted counts. Default-encoding filter guards do not guarantee
// selectivity under custom BSON options; callers must verify mutation filters.
type DeleteMapper[T Model] interface {
	// DeleteByID removes one ID match and returns its deleted count.
	// Strings become ObjectIDs unless the first notObjectID value is true.
	DeleteByID(id any, notObjectID ...bool) (int64, error)

	// DeleteByIDs removes ID matches and returns the deleted count; empty IDs return ErrEmptyIDs.
	// Strings become ObjectIDs unless the first notObjectID value is true.
	DeleteByIDs(ids []any, notObjectID ...bool) (int64, error)

	// DeleteOneByCond deletes one typed-filter match and returns the deleted count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	DeleteOneByCond(condition T) (int64, error)

	// DeleteOneByBSON rejects an empty map filter and returns the deleted count for one match.
	DeleteOneByBSON(condition bson.M) (int64, error)

	// DeleteByCond deletes all typed-filter matches and returns the deleted count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	DeleteByCond(condition T) (int64, error)

	// DeleteByBSON rejects an empty map filter and returns the deleted count for all matches.
	DeleteByBSON(condition bson.M) (int64, error)

	// DeleteOneWithOptions deletes one native-filter match using driver options and returns the deleted count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	DeleteOneWithOptions(filter any, opts ...options.Lister[options.DeleteOneOptions]) (int64, error)

	// DeleteWithOptions deletes all native-filter matches using driver options and returns the deleted count.
	// The empty-filter check uses default BSON encoding, not configured BSONOptions; verify filter selectivity explicitly.
	DeleteWithOptions(filter any, opts ...options.Lister[options.DeleteManyOptions]) (int64, error)
}

// Mapper aggregates raw collection access and query, insert, update, and delete APIs.
type Mapper[T Model] interface {
	RawMapper
	QueryMapper[T]
	InsertMapper[T]
	UpdateMapper[T]
	DeleteMapper[T]
}
