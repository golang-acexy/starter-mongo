package mongostarter

import (
	"slices"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// NewCondQuery 创建实体条件查询。
func NewCondQuery[T Model](condition T) CondQuery[T] {
	return CondQuery[T]{Condition: condition}
}

// NewBSONQuery 创建 BSON 条件查询。
func NewBSONQuery(condition bson.M) BSONQuery {
	return BSONQuery{Condition: condition}
}

// NewPageQuery 创建实体条件分页查询。
func NewPageQuery[T Model](condition T, number, size int) PageQuery[T] {
	return PageQuery[T]{Condition: condition, PageOptions: PageOptions{Number: number, Size: size}}
}

// NewBSONPageQuery 创建 BSON 条件分页查询。
func NewBSONPageQuery(condition bson.M, number, size int) BSONPageQuery {
	return BSONPageQuery{Condition: condition, PageOptions: PageOptions{Number: number, Size: size}}
}

// NewFilterPageQuery 创建原生 Filter 分页查询。
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

// WithOrderBy 设置实体条件查询的排序规则，再次调用会覆盖原值。
func (q CondQuery[T]) WithOrderBy(orderBy ...OrderBy) CondQuery[T] {
	q.QueryOptions = setQueryOrder(q.QueryOptions, orderBy)
	return q
}

// Select 设置实体条件查询的投影字段，再次调用会覆盖原值。
func (q CondQuery[T]) Select(columns ...string) CondQuery[T] {
	q.QueryOptions = setQuerySelect(q.QueryOptions, columns)
	return q
}

// WithLimit 设置实体条件列表查询的最大返回数量。
func (q CondQuery[T]) WithLimit(limit int) CondQuery[T] {
	q.QueryOptions = setQueryLimit(q.QueryOptions, limit)
	return q
}

// WithOrderBy 设置 BSON 条件查询的排序规则，再次调用会覆盖原值。
func (q BSONQuery) WithOrderBy(orderBy ...OrderBy) BSONQuery {
	q.QueryOptions = setQueryOrder(q.QueryOptions, orderBy)
	return q
}

// Select 设置 BSON 条件查询的投影字段，再次调用会覆盖原值。
func (q BSONQuery) Select(columns ...string) BSONQuery {
	q.QueryOptions = setQuerySelect(q.QueryOptions, columns)
	return q
}

// WithLimit 设置 BSON 条件列表查询的最大返回数量。
func (q BSONQuery) WithLimit(limit int) BSONQuery {
	q.QueryOptions = setQueryLimit(q.QueryOptions, limit)
	return q
}

// WithOrderBy 设置实体条件分页查询的排序规则，再次调用会覆盖原值。
func (q PageQuery[T]) WithOrderBy(orderBy ...OrderBy) PageQuery[T] {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select 设置实体条件分页查询的投影字段，再次调用会覆盖原值。
func (q PageQuery[T]) Select(columns ...string) PageQuery[T] {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions 设置实体条件分页查询的原生查询选项，再次调用会覆盖原值。
func (q PageQuery[T]) WithFindOptions(values ...options.Lister[options.FindOptions]) PageQuery[T] {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions 设置实体条件分页统计的原生选项，再次调用会覆盖原值。
func (q PageQuery[T]) WithCountOptions(values ...options.Lister[options.CountOptions]) PageQuery[T] {
	q.CountOptions = slices.Clone(values)
	return q
}

// WithOrderBy 设置 BSON 条件分页查询的排序规则，再次调用会覆盖原值。
func (q BSONPageQuery) WithOrderBy(orderBy ...OrderBy) BSONPageQuery {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select 设置 BSON 条件分页查询的投影字段，再次调用会覆盖原值。
func (q BSONPageQuery) Select(columns ...string) BSONPageQuery {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions 设置 BSON 条件分页查询的原生查询选项，再次调用会覆盖原值。
func (q BSONPageQuery) WithFindOptions(values ...options.Lister[options.FindOptions]) BSONPageQuery {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions 设置 BSON 条件分页统计的原生选项，再次调用会覆盖原值。
func (q BSONPageQuery) WithCountOptions(values ...options.Lister[options.CountOptions]) BSONPageQuery {
	q.CountOptions = slices.Clone(values)
	return q
}

// WithOrderBy 设置原生 Filter 分页查询的排序规则，再次调用会覆盖原值。
func (q FilterPageQuery) WithOrderBy(orderBy ...OrderBy) FilterPageQuery {
	q.PageOptions = setPageOrder(q.PageOptions, orderBy)
	return q
}

// Select 设置原生 Filter 分页查询的投影字段，再次调用会覆盖原值。
func (q FilterPageQuery) Select(columns ...string) FilterPageQuery {
	q.PageOptions = setPageSelect(q.PageOptions, columns)
	return q
}

// WithFindOptions 设置原生 Filter 分页查询的原生查询选项，再次调用会覆盖原值。
func (q FilterPageQuery) WithFindOptions(values ...options.Lister[options.FindOptions]) FilterPageQuery {
	q.FindOptions = slices.Clone(values)
	return q
}

// WithCountOptions 设置原生 Filter 分页统计的原生选项，再次调用会覆盖原值。
func (q FilterPageQuery) WithCountOptions(values ...options.Lister[options.CountOptions]) FilterPageQuery {
	q.CountOptions = slices.Clone(values)
	return q
}
