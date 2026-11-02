//go:build integration

package mongostarter

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var pressureCollectionName string

type pressureDocument struct {
	ID      int64  `bson:"_id,omitempty"`
	Payload string `bson:"payload"`
}

func (pressureDocument) CollectionName() string { return pressureCollectionName }

// TestMongoMemoryPressure writes only to a uniquely named collection and drops it during cleanup.
func TestMongoMemoryPressure(t *testing.T) {
	if os.Getenv("MONGO_MEMORY_STRESS") != "1" {
		t.Skip("set MONGO_MEMORY_STRESS=1 to allow isolated pressure data")
	}
	var getMore, killed atomic.Int64
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, e *event.CommandSucceededEvent) {
		switch e.CommandName {
		case "getMore":
			getMore.Add(1)
		case "killCursors":
			killed.Add(1)
		}
	}}
	client, observed := connectPool(t, PoolConfig{MaxPoolSize: new(uint64(8))}, monitor)
	database := "starter_mongo_pressure"
	pressureCollectionName = "pressure_" + bson.NewObjectID().Hex()
	mongoLock.Lock()
	if mongoRuntimeState.Load() != nil || mongoState != lifecycleStopped {
		mongoLock.Unlock()
		t.Fatal("runtime already active")
	}
	mongoRuntimeState.Store(&mongoRuntime{client: client, database: database})
	mongoState = lifecycleRunning
	mongoLock.Unlock()
	t.Cleanup(func() {
		mongoLock.Lock()
		mongoRuntimeState.Store(nil)
		mongoState = lifecycleStopped
		mongoLock.Unlock()
		pressureCollectionName = ""
	})
	collection := client.Database(database).Collection(pressureCollectionName)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := collection.Drop(ctx); err != nil {
			t.Error("could not remove owned pressure collection")
		}
	})
	var mapper BaseMapper[pressureDocument]
	documents := make([]*pressureDocument, 64)
	for i := range documents {
		documents[i] = &pressureDocument{ID: int64(i + 1), Payload: strings.Repeat("x", 1024)}
	}
	if ids, err := mapper.InsertBatch(documents); err != nil || len(ids) != 64 {
		t.Fatal("pressure fixture insert failed")
	}
	var build struct {
		Version string `bson:"version"`
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&build)
	cancel()
	if err == nil {
		t.Logf("server_version=%s", build.Version)
	}
	batch := func(mode string, count int) []weak.Pointer[pressureDocument] {
		var wg sync.WaitGroup
		var mu sync.Mutex
		refs := make([]weak.Pointer[pressureDocument], 0, count*16)
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				local := make([]weak.Pointer[pressureDocument], 0)
				for i := w; i < count; i += 16 {
					var rows []*pressureDocument
					var err error
					filter := bson.M{"_id": bson.M{"$gte": int64(1)}}
					opt := options.Find().SetBatchSize(4).SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(16)
					first := int64(1)
					switch mode {
					case "driver":
						var cursor *mongo.Cursor
						cursor, err = collection.Find(context.Background(), filter, opt)
						if err == nil {
							err = cursor.All(context.Background(), &rows)
						}
					case "mapper":
						err = mapper.SelectWithOptions(filter, &rows, opt)
					case "bson":
						err = mapper.SelectByBSON(NewBSONQuery(filter).WithLimit(16).WithOrderBy(OrderBy{Column: "_id"}), &rows)
					case "page":
						var total int64
						first = 17
						total, err = mapper.SelectPageByBSON(NewBSONPageQuery(filter, 2, 16).WithOrderBy(OrderBy{Column: "_id"}).WithFindOptions(options.Find().SetBatchSize(4)), &rows)
						if err == nil && total != 64 {
							err = fmt.Errorf("unexpected count: %d", total)
						}
					}
					if err != nil {
						t.Errorf("%s failed (%T)", mode, err)
						return
					}
					if len(rows) != 16 {
						t.Errorf("unexpected result count %d", len(rows))
						return
					}
					for j, row := range rows {
						if row == nil || row.ID != first+int64(j) || len(row.Payload) != 1024 {
							t.Error("invalid result")
							return
						}
						local = append(local, weak.Make(row))
					}
				}
				mu.Lock()
				refs = append(refs, local...)
				mu.Unlock()
			}(w)
		}
		wg.Wait()
		return refs
	}
	for _, mode := range []string{"driver", "mapper", "bson", "page"} {
		t.Run(mode, func(t *testing.T) {
			batch(mode, 100)
			runtime.GC()
			runtime.GC()
			var baseline runtime.MemStats
			runtime.ReadMemStats(&baseline)
			baseG := runtime.NumGoroutine()
			for n := 1; n <= 5; n++ {
				start := time.Now()
				refs := batch(mode, 500)
				elapsed := time.Since(start)
				runtime.GC()
				runtime.GC()
				live := 0
				for _, ref := range refs {
					if ref.Value() != nil {
						live++
					}
				}
				var memory runtime.MemStats
				runtime.ReadMemStats(&memory)
				observed.mu.Lock()
				out, open := observed.checkedOut, observed.created-observed.closed
				observed.mu.Unlock()
				t.Logf("batch=%d ops_s=%.0f heap=%d heap_delta=%d allocated=%d live=%d checked_out=%d open=%d goroutines=%d baseline_goroutines=%d", n, 500/elapsed.Seconds(), memory.HeapAlloc, int64(memory.HeapAlloc)-int64(baseline.HeapAlloc), memory.TotalAlloc-baseline.TotalAlloc, live, out, open, runtime.NumGoroutine(), baseG)
				if live != 0 || out != 0 || open > 8 || len(refs) != 8000 {
					t.Fatal("resource retention or incomplete batch")
				}
			}
		})
	}
	t.Run("shared_page_options", func(t *testing.T) {
		for _, mode := range []string{"typed", "bson", "native"} {
			t.Run(mode, func(t *testing.T) {
				marker := options.Find().SetLimit(3)
				backing := []options.Lister[options.FindOptions]{options.Find().SetLimit(1).SetSkip(40).SetBatchSize(4), marker, marker, marker}
				page := PageOptions{Number: 2, Size: 16, FindOptions: backing[:1], SelectColumns: []string{"_id", "payload"}, OrderBy: NewOrderBy("_id", false)}
				run := func() {
					var rows []*pressureDocument
					var total int64
					var err error
					switch mode {
					case "typed":
						total, err = mapper.SelectPageByCond(PageQuery[pressureDocument]{Condition: pressureDocument{Payload: strings.Repeat("x", 1024)}, PageOptions: page}, &rows)
					case "bson":
						total, err = mapper.SelectPageByBSON(BSONPageQuery{Condition: bson.M{}, PageOptions: page}, &rows)
					case "native":
						total, err = mapper.SelectPageWithOptions(FilterPageQuery{Filter: bson.D{}, PageOptions: page}, &rows)
					}
					if err != nil || total != 64 || len(rows) != 16 || rows[0].ID != 17 || rows[15].ID != 32 || len(rows[0].Payload) != 1024 {
						t.Error("page result or option precedence changed")
					}
				}
				// The typed filter omits the zero ID through the tag on the fixture model.
				run()
				for _, value := range backing[1:] {
					if value != marker {
						t.Fatal("pagination overwrote caller option backing storage")
					}
				}
				var wg sync.WaitGroup
				for range 16 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for range 10 {
							run()
						}
					}()
				}
				wg.Wait()
				for _, value := range backing[1:] {
					if value != marker {
						t.Fatal("concurrent pagination overwrote caller storage")
					}
				}
			})
		}
	})
	t.Run("failed_decode", func(t *testing.T) {
		before := killed.Load()
		for i := 0; i < 50; i++ {
			cursor, err := collection.Find(context.Background(), bson.D{}, options.Find().SetBatchSize(1))
			if err != nil {
				t.Fatal("find failed")
			}
			if cursor.ID() == 0 {
				cursor.Close(context.Background())
				t.Fatal("expected a server cursor")
			}
			var invalid []struct {
				Payload int `bson:"payload"`
			}
			if err = checkMultipleResult(cursor, nil, &invalid); err == nil || cursor.ID() != 0 {
				t.Fatal("failed decode did not close cursor")
			}
		}
		if killed.Load()-before != 50 {
			t.Fatal("server cursor cleanup commands missing")
		}
	})
	t.Run("invalid_destination", func(t *testing.T) {
		cursor, err := collection.Find(context.Background(), bson.D{}, options.Find().SetBatchSize(1))
		if err != nil {
			t.Fatal("find failed")
		}
		if err = checkMultipleResult(cursor, nil, 42); err == nil || cursor.ID() != 0 {
			t.Fatal("invalid destination did not close cursor")
		}
	})
	if getMore.Load() == 0 {
		t.Fatal("multi-batch path not exercised")
	}
	t.Logf("getMore=%d killCursors=%d", getMore.Load(), killed.Load())
	observed.mu.Lock()
	out := observed.checkedOut
	observed.mu.Unlock()
	if out != 0 {
		t.Fatal("checked-out connection remains")
	}
}

func pressureLifecycle(t *testing.T) weak.Pointer[mongo.Client] {
	t.Helper()
	starter := &MongoStarter{Config: MongoConfig{MongoURI: poolIntegrationURI(t), Database: "starter_mongo_pressure"}}
	instance, err := starter.Start()
	if err != nil {
		t.Fatal("lifecycle startup failed")
	}
	ref := weak.Make(instance.(*mongo.Client))
	graceful, stopped, err := starter.Stop(5 * time.Second)
	if err != nil || !graceful || !stopped || RawMongoClient() != nil {
		t.Fatal("lifecycle stop failed")
	}
	return ref
}

func TestMongoPressureLifecycle(t *testing.T) {
	if os.Getenv("MONGO_MEMORY_STRESS") != "1" {
		t.Skip("set MONGO_MEMORY_STRESS=1")
	}
	refs := make([]weak.Pointer[mongo.Client], 0, 20)
	for i := 0; i < 20; i++ {
		refs = append(refs, pressureLifecycle(t))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		runtime.GC()
		live := 0
		for _, ref := range refs {
			if ref.Value() != nil {
				live++
			}
		}
		if live == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained clients: %d", live)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
