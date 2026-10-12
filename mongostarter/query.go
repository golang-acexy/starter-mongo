package mongostarter

import (
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// NewCondQuery stores a typed filter without validation; reference-bearing fields remain borrowed.
func NewCondQuery[T Model](condition T) CondQuery[T] {
	return CondQuery[T]{Condition: condition}
}

// NewBSONQuery borrows the BSON filter map; do not mutate it during execution.
func NewBSONQuery(condition bson.M) BSONQuery {
	return BSONQuery{Condition: condition}
}

// NewPageQuery stores a typed filter and page inputs; validation is deferred until execution.
func NewPageQuery[T Model](condition T, number, size int) PageQuery[T] {
	return PageQuery[T]{Condition: condition, PageOptions: PageOptions{Number: number, Size: size}}
}

// NewBSONPageQuery borrows the BSON filter and stores page inputs without validating them.
func NewBSONPageQuery(condition bson.M, number, size int) BSONPageQuery {
	return BSONPageQuery{Condition: condition, PageOptions: PageOptions{Number: number, Size: size}}
}

// NewFilterPageQuery borrows the native filter and stores page inputs without validating them.
func NewFilterPageQuery(filter any, number, size int) FilterPageQuery {
	return FilterPageQuery{Filter: filter, PageOptions: PageOptions{Number: number, Size: size}}
}

func setQueryOrder(options QueryOptions, orderBy []OrderBy) QueryOptions {
	options.OrderBy = NewOrderBys(orderBy...)
	return options
}

func setQuerySelect(options QueryOptions, columns []string) QueryOptions {
	options.SelectColumns = slices.Clone(columns)
	return options
}

func setQueryLimit(options QueryOptions, limit int) QueryOptions {
	options.Limit = limit
	return options
}

func setPageOrder(options PageOptions, orderBy []OrderBy) PageOptions {
	options.OrderBy = NewOrderBys(orderBy...)
	return options
}

func setPageSelect(options PageOptions, columns []string) PageOptions {
	options.SelectColumns = slices.Clone(columns)
	return options
}

// WithOrderBy returns a query copy with copied sort rules replacing previous rules; validation is deferred.
func (q CondQuery[T]) WithOrderBy(orderBy ...OrderBy) CondQuery[T] {
	q.QueryOptions = setQueryOrder(q.QueryOptions, orderBy)
	return q
}

// Select returns a query copy with copied inclusion fields replacing previous fields; _id requires explicit selection.
func (q CondQuery[T]) Select(columns ...string) CondQuery[T] {
	q.QueryOptions = setQuerySelect(q.QueryOptions, columns)
	return q
}

// WithLimit returns a query copy with a list limit; zero is unlimited and negatives fail at execution.
func (q CondQuery[T]) WithLimit(limit int) CondQuery[T] {
	q.QueryOptions = setQueryLimit(q.QueryOptions, limit)
	return q
}

// WithOrderBy returns a query copy with copied sort rules replacing previous rules; validation is deferred.
func (q BSONQuery) WithOrderBy(orderBy ...OrderBy) BSONQuery {
	q.QueryOptions = setQueryOrder(q.QueryOptions, orderBy)
	return q
}

// Select returns a query copy with copied inclusion fields replacing previous fields; _id requires explicit selection.
func (q BSONQuery) Select(columns ...string) BSONQuery {
	q.QueryOptions = setQuerySelect(q.QueryOptions, columns)
	return q
}

// WithLimit returns a query copy with a list limit; zero is unlimited and negatives fail at execution.
func (q BSONQuery) WithLimit(limit int) BSONQuery {
	q.QueryOptions = setQueryLimit(q.QueryOptions, limit)
	return q
}

// WithOrderBy returns a query copy with copied sort rules replacing previous rules; validation is deferred.
func (q PageQuery[T]) WithOrderBy(orderBy ...OrderBy) PageQuery[T] {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select returns a query copy with copied inclusion fields replacing previous fields; _id requires explicit selection.
func (q PageQuery[T]) Select(columns ...string) PageQuery[T] {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q PageQuery[T]) WithFindOptions(values ...options.Lister[options.FindOptions]) PageQuery[T] {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q PageQuery[T]) WithCountOptions(values ...options.Lister[options.CountOptions]) PageQuery[T] {
	q.CountOptions = slices.Clone(values)
	return q
}

// WithOrderBy returns a query copy with copied sort rules replacing previous rules; validation is deferred.
func (q BSONPageQuery) WithOrderBy(orderBy ...OrderBy) BSONPageQuery {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select returns a query copy with copied inclusion fields replacing previous fields; _id requires explicit selection.
func (q BSONPageQuery) Select(columns ...string) BSONPageQuery {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q BSONPageQuery) WithFindOptions(values ...options.Lister[options.FindOptions]) BSONPageQuery {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q BSONPageQuery) WithCountOptions(values ...options.Lister[options.CountOptions]) BSONPageQuery {
	q.CountOptions = slices.Clone(values)
	return q
}

// WithOrderBy returns a query copy with copied sort rules replacing previous rules; validation is deferred.
func (q FilterPageQuery) WithOrderBy(orderBy ...OrderBy) FilterPageQuery {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select returns a query copy with copied inclusion fields replacing previous fields; _id requires explicit selection.
func (q FilterPageQuery) Select(columns ...string) FilterPageQuery {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q FilterPageQuery) WithFindOptions(values ...options.Lister[options.FindOptions]) FilterPageQuery {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions copies the option slice, replacing previous values; option implementations remain shared.
func (q FilterPageQuery) WithCountOptions(values ...options.Lister[options.CountOptions]) FilterPageQuery {
	q.CountOptions = slices.Clone(values)
	return q
}
