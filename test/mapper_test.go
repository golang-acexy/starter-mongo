package test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-acexy/starter-mongo/mongostarter"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const testCollection = "starter_mongo_integration_test"

type StartupLog struct {
	ID        string                 `bson:"_id,omitempty" json:"id"`
	PID       int                    `bson:"pid,omitempty" json:"pid"`
	Hostname  string                 `bson:"hostname,omitempty" json:"hostname"`
	StartTime mongostarter.Timestamp `bson:"startTime,omitempty" json:"startTime"`
}

func (StartupLog) CollectionName() string {
	return testCollection
}

type StartupLogMapper struct {
	mongostarter.BaseMapper[StartupLog]
}

type BooleanIDLog struct {
	ID       bool   `bson:"_id"`
	Hostname string `bson:"hostname"`
}

func (BooleanIDLog) CollectionName() string {
	return testCollection
}

type BooleanIDLogMapper struct {
	mongostarter.BaseMapper[BooleanIDLog]
}

var mapper StartupLogMapper
var booleanIDMapper BooleanIDLogMapper

func resetCollection(t *testing.T) {
	t.Helper()
	collection := mongostarter.RawCollection(testCollection)
	if collection == nil {
		t.Fatal("mongo collection is not initialized")
	}
	if _, err := collection.DeleteMany(t.Context(), bson.M{}); err != nil {
		t.Fatal(err)
	}
}

func insertLog(t *testing.T, hostname string, pid int) string {
	t.Helper()
	id, err := mapper.Insert(&StartupLog{
		PID:       pid,
		Hostname:  hostname,
		StartTime: mongostarter.Timestamp{Time: time.Now().UTC().Truncate(time.Millisecond)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected generated ObjectID")
	}
	return id
}

func TestInsertSelectUpdateDeleteByID(t *testing.T) {
	resetCollection(t)
	id := insertLog(t, "node-a", 1)

	var selected StartupLog
	if err := mapper.SelectByID(id, &selected); err != nil {
		t.Fatal(err)
	}
	if selected.Hostname != "node-a" || selected.PID != 1 {
		t.Fatalf("unexpected inserted document: %+v", selected)
	}

	modified, err := mapper.UpdateByIDWithBSON(bson.M{"hostname": "node-b", "pid": 2}, id)
	if err != nil {
		t.Fatal(err)
	}
	if modified != 1 {
		t.Fatalf("expected 1 modified document, got %d", modified)
	}
	if err = mapper.SelectByID(id, &selected); err != nil {
		t.Fatal(err)
	}
	if selected.Hostname != "node-b" || selected.PID != 2 {
		t.Fatalf("unexpected updated document: %+v", selected)
	}

	deleted, err := mapper.DeleteByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted document, got %d", deleted)
	}
	if err = mapper.SelectByID(id, &selected); !errors.Is(err, mongo.ErrNoDocuments) {
		t.Fatalf("expected mongo.ErrNoDocuments, got %v", err)
	}
}

func TestNonObjectID(t *testing.T) {
	resetCollection(t)
	if _, err := booleanIDMapper.Insert(&BooleanIDLog{ID: true, Hostname: "boolean-id"}); err != nil {
		t.Fatal(err)
	}
	var selected BooleanIDLog
	if err := booleanIDMapper.SelectByID(true, &selected, true); err != nil {
		t.Fatal(err)
	}
	if !selected.ID || selected.Hostname != "boolean-id" {
		t.Fatalf("unexpected document: %+v", selected)
	}
	deleted, err := booleanIDMapper.DeleteByID(true, true)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted document, got %d", deleted)
	}
}

func TestBatchAndSelectByIDs(t *testing.T) {
	resetCollection(t)
	ids, err := mapper.InsertBatch([]*StartupLog{
		{Hostname: "batch-a", PID: 1},
		{Hostname: "batch-b", PID: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 inserted IDs, got %d", len(ids))
	}

	var selected []*StartupLog
	if err = mapper.SelectByIDs([]any{ids[0], ids[1]}, &selected); err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected documents, got %d", len(selected))
	}

	bsonIDs, err := mapper.InsertBatchWithBSON(bson.A{
		bson.M{"hostname": "bson-a", "pid": 3},
		bson.D{{Key: "hostname", Value: "bson-b"}, {Key: "pid", Value: 4}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bsonIDs) != 2 {
		t.Fatalf("expected 2 BSON inserted IDs, got %d", len(bsonIDs))
	}
}

func TestQueryVariantsAndPagination(t *testing.T) {
	resetCollection(t)
	for index, hostname := range []string{"node-a", "node-a", "node-b", "node-b", "node-c"} {
		insertLog(t, hostname, index+1)
	}

	var one StartupLog
	if err := mapper.SelectOneByCond(&StartupLog{Hostname: "node-a"}, &one); err != nil {
		t.Fatal(err)
	}
	if one.Hostname != "node-a" {
		t.Fatalf("unexpected condition result: %+v", one)
	}
	one = StartupLog{}
	if err := mapper.SelectOneByBSON(bson.M{"hostname": "node-b"}, &one, "hostname"); err != nil {
		t.Fatal(err)
	}
	if one.Hostname != "node-b" || one.ID != "" {
		t.Fatalf("projection was not applied: %+v", one)
	}
	one = StartupLog{}
	if err := mapper.SelectOneWithOptions(bson.M{"pid": 5}, &one, options.FindOne().SetProjection(bson.M{"hostname": 1})); err != nil {
		t.Fatal(err)
	}
	if one.Hostname != "node-c" {
		t.Fatalf("unexpected options result: %+v", one)
	}

	var logs []*StartupLog
	orders := mongostarter.NewOrderBys(
		mongostarter.OrderBy{Column: "hostname"},
		mongostarter.OrderBy{Column: "pid", Desc: true},
	)
	if err := mapper.SelectByBSON(bson.M{}, orders, &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 5 || logs[0].Hostname != "node-a" || logs[0].PID != 2 {
		t.Fatalf("unexpected multi-field sort result: %+v", logs)
	}

	total, err := mapper.SelectPageByBSON(
		bson.M{},
		mongostarter.PageQuery{PageNumber: 2, PageSize: 2, OrderBy: mongostarter.NewOrderBy("pid", false)},
		&logs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(logs) != 2 || logs[0].PID != 3 {
		t.Fatalf("unexpected page result: total=%d logs=%+v", total, logs)
	}

	total, err = mapper.SelectPageWithOptions(
		bson.M{"hostname": "node-b"},
		mongostarter.PageQuery{
			PageNumber:   1,
			PageSize:     1,
			FindOptions:  []options.Lister[options.FindOptions]{options.Find().SetProjection(bson.M{"hostname": 1})},
			CountOptions: []options.Lister[options.CountOptions]{options.Count().SetComment("integration-test")},
		},
		&logs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(logs) != 1 || logs[0].Hostname != "node-b" {
		t.Fatalf("unexpected options page result: total=%d logs=%+v", total, logs)
	}
}

func TestUpdateAndDeleteByCondition(t *testing.T) {
	resetCollection(t)
	insertLog(t, "target", 1)
	insertLog(t, "target", 2)
	insertLog(t, "other", 3)

	modified, err := mapper.UpdateOneByCond(&StartupLog{PID: 10}, &StartupLog{Hostname: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if modified != 1 {
		t.Fatalf("expected 1 modified document, got %d", modified)
	}
	modified, err = mapper.UpdateByBSON(bson.M{"pid": 20}, bson.M{"hostname": "target"})
	if err != nil {
		t.Fatal(err)
	}
	if modified != 2 {
		t.Fatalf("expected 2 modified documents, got %d", modified)
	}

	deleted, err := mapper.DeleteOneByBSON(bson.M{"hostname": "target"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted document, got %d", deleted)
	}
	deleted, err = mapper.DeleteByCond(&StartupLog{Hostname: "target"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted document, got %d", deleted)
	}
}

func TestValidationErrors(t *testing.T) {
	resetCollection(t)
	var logs []*StartupLog
	if err := mapper.SelectByIDs(nil, &logs); !errors.Is(err, mongostarter.ErrEmptyIDs) {
		t.Fatalf("expected ErrEmptyIDs, got %v", err)
	}
	if _, err := mapper.SelectPageByCond(&StartupLog{}, mongostarter.PageQuery{}, &logs); !errors.Is(err, mongostarter.ErrInvalidPage) {
		t.Fatalf("expected ErrInvalidPage, got %v", err)
	}
	if _, err := mapper.UpdateByBSON(bson.M{"pid": 1}, bson.M{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.DeleteByBSON(bson.M{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.UpdateOneByCond(&StartupLog{PID: 1}, &StartupLog{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.DeleteOneByCond(&StartupLog{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := (&mongostarter.MongoStarter{}).Start(); !errors.Is(err, mongostarter.ErrMongoStarterAlreadyStarted) {
		t.Fatalf("expected ErrMongoStarterAlreadyStarted, got %v", err)
	}
}

func TestTimestampBSON(t *testing.T) {
	var zero mongostarter.Timestamp
	typ, data, err := zero.MarshalBSONValue()
	if err != nil {
		t.Fatal(err)
	}
	if bson.Type(typ) != bson.TypeNull || data != nil {
		t.Fatalf("unexpected zero timestamp encoding: type=%v data=%v", bson.Type(typ), data)
	}
	if err = zero.UnmarshalBSONValue(byte(bson.TypeNull), nil); err != nil {
		t.Fatal(err)
	}

	want := time.Now().UTC().Truncate(time.Millisecond)
	value := mongostarter.Timestamp{Time: want}
	typ, data, err = value.MarshalBSONValue()
	if err != nil {
		t.Fatal(err)
	}
	var decoded mongostarter.Timestamp
	if err = decoded.UnmarshalBSONValue(typ, data); err != nil {
		t.Fatal(err)
	}
	if !decoded.Time.Equal(want) {
		t.Fatalf("timestamp mismatch: want=%v got=%v", want, decoded.Time)
	}
}
