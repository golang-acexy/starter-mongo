package mongostarter

type IBaseModel interface {
	CollectionName() string
}

// BaseMapper 接口声明
type BaseMapper[T IBaseModel] struct {
	Value T
}

// OrderBy 排序规则
type OrderBy struct {
	Column string
	Desc   bool
}

// NewOrderBy 新增排序规则
func NewOrderBy(column string, desc bool) []*OrderBy {
	return []*OrderBy{{Column: column, Desc: desc}}
}
