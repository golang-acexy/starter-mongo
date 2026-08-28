package mongostarter

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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

func TestNormalizeBSONUpdate(t *testing.T) {
	plain := bson.M{"name": "updated"}
	normalized := normalizeBSONUpdate(plain)
	if !reflect.DeepEqual(normalized, bson.M{"$set": plain}) {
		t.Fatalf("普通字段更新应自动包装 $set: %v", normalized)
	}

	operator := bson.M{"$inc": bson.M{"count": 1}}
	if normalized = normalizeBSONUpdate(operator); !reflect.DeepEqual(normalized, operator) {
		t.Fatalf("MongoDB 更新操作符不应被再次包装: %v", normalized)
	}
}

func TestPageOffsetValidation(t *testing.T) {
	offset, err := pageOffset(3, 20)
	if err != nil || offset != 40 {
		t.Fatalf("分页偏移计算异常: offset=%d err=%v", offset, err)
	}
	if _, err = pageOffset(0, 20); !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("无效页码应返回 ErrInvalidPage: %v", err)
	}
	if _, err = pageOffset(math.MaxInt, math.MaxInt); !errors.Is(err, ErrInvalidPage) {
		t.Fatalf("溢出分页参数应返回 ErrInvalidPage: %v", err)
	}
}

func TestBuildSortValidation(t *testing.T) {
	if _, err := buildSort([]*OrderBy{nil}); !errors.Is(err, ErrInvalidOrderBy) {
		t.Fatalf("nil 排序规则应返回 ErrInvalidOrderBy: %v", err)
	}
	if _, err := buildSort([]*OrderBy{{Column: " "}}); !errors.Is(err, ErrInvalidOrderBy) {
		t.Fatalf("空排序字段应返回 ErrInvalidOrderBy: %v", err)
	}

	sort, err := buildSort(NewOrderBys(OrderBy{Column: "name"}, OrderBy{Column: "createdAt", Desc: true}))
	if err != nil || !reflect.DeepEqual(sort, bson.D{{Key: "name", Value: 1}, {Key: "createdAt", Value: -1}}) {
		t.Fatalf("排序构建异常: sort=%v err=%v", sort, err)
	}
}

func TestMongoConfigSnapshot(t *testing.T) {
	compressors := []string{"zstd"}
	bsonOptions := &options.BSONOptions{UseJSONStructTags: true}
	starter := &MongoStarter{Config: MongoConfig{Compressors: compressors, BSONOptions: bsonOptions}}

	config := starter.getConfig()
	compressors[0] = "snappy"
	bsonOptions.UseJSONStructTags = false

	if !reflect.DeepEqual(config.Compressors, []string{"zstd"}) {
		t.Fatalf("压缩算法配置未被复制: %v", config.Compressors)
	}
	if !config.BSONOptions.UseJSONStructTags {
		t.Fatal("BSON 配置未被复制")
	}
}

func TestMongoRuntimeSnapshotPublication(t *testing.T) {
	previous := mongoRuntimeState.Swap(nil)
	defer mongoRuntimeState.Store(previous)

	client := new(mongo.Client)
	mongoRuntimeState.Store(&mongoRuntime{client: client, database: "app"})
	if RawMongoClient() != client {
		t.Fatal("原始客户端未从运行时快照读取")
	}
	if runtime := mongoRuntimeState.Load(); runtime == nil || runtime.database != "app" {
		t.Fatalf("运行时快照不完整: %+v", runtime)
	}

	mongoRuntimeState.Store(nil)
	if RawMongoClient() != nil || RawDatabase() != nil {
		t.Fatal("摘除运行时快照后不应继续暴露 MongoDB 资源")
	}
}
