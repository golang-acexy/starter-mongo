package mongostarter

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type queryModel struct{}

func (queryModel) CollectionName() string { return "query_models" }

func TestQueryBuilders(t *testing.T) {
	orderBy := OrderBy{Column: "created_at", Desc: true}
	cond := NewCondQuery(queryModel{}).WithOrderBy(orderBy).Select("name").WithLimit(10)
	if cond.Limit != 10 || len(cond.OrderBy) != 1 || cond.OrderBy[0].Column != "created_at" || !reflect.DeepEqual(cond.SelectColumns, []string{"name"}) {
		t.Fatalf("实体条件查询构建异常: %+v", cond)
	}

	bsonQuery := NewBSONQuery(bson.M{"status": 0}).WithOrderBy(orderBy).Select("status").WithLimit(5)
	if bsonQuery.Limit != 5 || len(bsonQuery.OrderBy) != 1 || !reflect.DeepEqual(bsonQuery.SelectColumns, []string{"status"}) {
		t.Fatalf("BSON 条件查询构建异常: %+v", bsonQuery)
	}

	page := NewPageQuery(queryModel{}, 2, 20).WithOrderBy(orderBy).Select("name")
	if page.Number != 2 || page.Size != 20 || len(page.OrderBy) != 1 || !reflect.DeepEqual(page.SelectColumns, []string{"name"}) {
		t.Fatalf("实体条件分页查询构建异常: %+v", page)
	}

	bsonPage := NewBSONPageQuery(bson.M{"status": 0}, 1, 10).WithOrderBy(orderBy).Select("status")
	if bsonPage.Number != 1 || bsonPage.Size != 10 || len(bsonPage.OrderBy) != 1 {
		t.Fatalf("BSON 条件分页查询构建异常: %+v", bsonPage)
	}

	filterPage := NewFilterPageQuery(bson.M{}, 3, 30).WithOrderBy(orderBy).Select("name")
	if filterPage.Number != 3 || filterPage.Size != 30 || len(filterPage.OrderBy) != 1 {
		t.Fatalf("Filter 分页查询构建异常: %+v", filterPage)
	}
}
