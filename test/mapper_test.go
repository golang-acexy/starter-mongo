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
	collection := mapper.Collection()
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

func TestInsertVariants(t *testing.T) {
	resetCollection(t)
	id, err := mapper.InsertWithBSON(bson.M{"hostname": "bson-one", "pid": 1})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected BSON insert ID")
	}

	id, err = mapper.InsertWithOptions(
		bson.M{"hostname": "options-one", "pid": 2},
		options.InsertOne().SetBypassDocumentValidation(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("expected options insert ID")
	}

	ids, err := mapper.InsertBatchWithOptions(
		[]any{
			bson.M{"hostname": "options-batch", "pid": 3},
			bson.M{"hostname": "options-batch", "pid": 4},
		},
		options.InsertMany().SetBypassDocumentValidation(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 options insert IDs, got %d", len(ids))
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

func TestExistsAndDeleteByIDs(t *testing.T) {
	resetCollection(t)
	ids, err := mapper.InsertBatch([]*StartupLog{
		{Hostname: "exists-a", PID: 1},
		{Hostname: "exists-b", PID: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		exists, existsErr := mapper.ExistsByID(id)
		if existsErr != nil {
			t.Fatal(existsErr)
		}
		if !exists {
			t.Fatalf("expected document %s to exist", id)
		}
	}

	deleted, err := mapper.DeleteByIDs([]any{ids[0], ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted documents, got %d", deleted)
	}
	exists, err := mapper.ExistsByID(ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("expected deleted document not to exist")
	}
}

func TestDeleteByIDsWithNonObjectID(t *testing.T) {
	resetCollection(t)
	if _, err := booleanIDMapper.Insert(&BooleanIDLog{ID: true, Hostname: "boolean-true"}); err != nil {
		t.Fatal(err)
	}
	if _, err := booleanIDMapper.Insert(&BooleanIDLog{ID: false, Hostname: "boolean-false"}); err != nil {
		t.Fatal(err)
	}
	deleted, err := booleanIDMapper.DeleteByIDs([]any{true, false}, true)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted documents, got %d", deleted)
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

	total, err = mapper.SelectPageByCond(
		&StartupLog{Hostname: "node-a"},
		mongostarter.PageQuery{PageNumber: 1, PageSize: 1, OrderBy: mongostarter.NewOrderBy("pid", true)},
		&logs,
	)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(logs) != 1 || logs[0].PID != 2 {
		t.Fatalf("unexpected condition page result: total=%d logs=%+v", total, logs)
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

func TestListAndCountVariants(t *testing.T) {
	resetCollection(t)
	insertLog(t, "count-a", 1)
	insertLog(t, "count-a", 2)
	insertLog(t, "count-b", 3)

	var logs []*StartupLog
	if err := mapper.SelectByCond(&StartupLog{Hostname: "count-a"}, mongostarter.NewOrderBy("pid", true), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0].PID != 2 {
		t.Fatalf("unexpected condition list: %+v", logs)
	}

	logs = nil
	if err := mapper.SelectWithOptions(
		bson.M{"hostname": "count-a"},
		&logs,
		options.Find().SetSort(bson.D{{Key: "pid", Value: 1}}).SetProjection(bson.M{"pid": 1}),
	); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0].PID != 1 || logs[0].Hostname != "" {
		t.Fatalf("unexpected options list: %+v", logs)
	}

	count, err := mapper.CountByCond(&StartupLog{Hostname: "count-a"})
	if err != nil || count != 2 {
		t.Fatalf("unexpected condition count: count=%d err=%v", count, err)
	}
	count, err = mapper.CountByBSON(bson.M{"hostname": "count-b"})
	if err != nil || count != 1 {
		t.Fatalf("unexpected BSON count: count=%d err=%v", count, err)
	}
	count, err = mapper.CountWithOptions(bson.M{}, options.Count().SetComment("count-options"))
	if err != nil || count != 3 {
		t.Fatalf("unexpected options count: count=%d err=%v", count, err)
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

func TestRemainingUpdateAndDeleteVariants(t *testing.T) {
	resetCollection(t)
	id := insertLog(t, "variant-id", 1)
	insertLog(t, "variant-one", 2)
	insertLog(t, "variant-many", 3)
	insertLog(t, "variant-many", 4)

	modified, err := mapper.UpdateByID(&StartupLog{Hostname: "variant-id-updated", PID: 10}, id)
	if err != nil || modified != 1 {
		t.Fatalf("unexpected ID update: modified=%d err=%v", modified, err)
	}
	modified, err = mapper.UpdateOneByBSON(bson.M{"pid": 20}, bson.M{"hostname": "variant-one"})
	if err != nil || modified != 1 {
		t.Fatalf("unexpected BSON single update: modified=%d err=%v", modified, err)
	}
	modified, err = mapper.UpdateByCond(&StartupLog{PID: 30}, &StartupLog{Hostname: "variant-many"})
	if err != nil || modified != 2 {
		t.Fatalf("unexpected condition update: modified=%d err=%v", modified, err)
	}

	deleted, err := mapper.DeleteOneByCond(&StartupLog{Hostname: "variant-one"})
	if err != nil || deleted != 1 {
		t.Fatalf("unexpected condition single delete: deleted=%d err=%v", deleted, err)
	}
	deleted, err = mapper.DeleteByBSON(bson.M{"hostname": "variant-many"})
	if err != nil || deleted != 2 {
		t.Fatalf("unexpected BSON delete: deleted=%d err=%v", deleted, err)
	}
}

func TestUpdateAndDeleteWithOptions(t *testing.T) {
	resetCollection(t)
	insertLog(t, "options-a", 1)
	insertLog(t, "options-b", 2)
	insertLog(t, "options-b", 3)

	modified, err := mapper.UpdateOneWithOptions(
		bson.M{"hostname": "options-a"},
		bson.M{"$set": bson.M{"pid": 10}},
		options.UpdateOne().SetComment("update-one"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if modified != 1 {
		t.Fatalf("expected 1 modified document, got %d", modified)
	}

	modified, err = mapper.UpdateWithOptions(
		bson.M{"hostname": "options-b"},
		bson.M{"$set": bson.M{"pid": 20}},
		options.UpdateMany().SetComment("update-many"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if modified != 2 {
		t.Fatalf("expected 2 modified documents, got %d", modified)
	}

	if _, err = mapper.UpdateOneWithOptions(
		bson.M{"hostname": "options-upsert"},
		bson.M{"$set": bson.M{"pid": 30}},
		options.UpdateOne().SetUpsert(true),
	); err != nil {
		t.Fatal(err)
	}
	var upserted StartupLog
	if err = mapper.SelectOneByBSON(bson.M{"hostname": "options-upsert"}, &upserted); err != nil {
		t.Fatal(err)
	}
	if upserted.PID != 30 {
		t.Fatalf("unexpected upserted document: %+v", upserted)
	}

	deleted, err := mapper.DeleteOneWithOptions(bson.M{"hostname": "options-a"}, options.DeleteOne().SetComment("delete-one"))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted document, got %d", deleted)
	}
	deleted, err = mapper.DeleteWithOptions(bson.M{"hostname": "options-b"}, options.DeleteMany().SetComment("delete-many"))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted documents, got %d", deleted)
	}
}

func TestValidationErrors(t *testing.T) {
	resetCollection(t)
	var logs []*StartupLog
	if err := mapper.SelectByIDs(nil, &logs); !errors.Is(err, mongostarter.ErrEmptyIDs) {
		t.Fatalf("expected ErrEmptyIDs, got %v", err)
	}
	if _, err := mapper.DeleteByIDs(nil); !errors.Is(err, mongostarter.ErrEmptyIDs) {
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
	if _, err := mapper.UpdateOneWithOptions(bson.M{}, bson.M{"$set": bson.M{"pid": 1}}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.UpdateWithOptions(bson.M{}, bson.M{"$set": bson.M{"pid": 1}}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.DeleteOneWithOptions(bson.M{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
		t.Fatalf("expected ErrEmptyCondition, got %v", err)
	}
	if _, err := mapper.DeleteWithOptions(bson.M{}); !errors.Is(err, mongostarter.ErrEmptyCondition) {
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
