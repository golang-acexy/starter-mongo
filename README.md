# starter-mongo

`starter-mongo` brings the official MongoDB Go driver into the golang-acexy starter lifecycle. It manages one application-wide client and provides generic Mapper APIs for common document operations.

## Highlights

- One lifecycle-managed MongoDB client and default database.
- `BaseMapper[T]` covers query, insert, update, delete, count, and pagination operations.
- Typed models, `bson.M`, and native driver filters/options can be used side by side.
- String IDs support both MongoDB `ObjectID` and ordinary string values.
- Multi-field sorting, projection, and native find/count options are built into pagination.
- Mutation APIs include empty-filter checks; review the custom BSON encoding limitation below.
- Write APIs return acknowledged insert IDs or affected document counts.
- Narrow raw accessors remain available for advanced driver operations.

`cloud-database/mongo` builds business-oriented Repository APIs on top of this module; client lifecycle remains owned by `starter-mongo`.

## Requirements

- Go `1.26.7`

## Installation

```bash
go get github.com/golang-acexy/starter-mongo
```

## Quick Start

Register `MongoStarter` with the parent loader:

```go
starter := &mongostarter.MongoStarter{
	Config: mongostarter.MongoConfig{
		MongoURI: "mongodb://YOUR_USERNAME:YOUR_PASSWORD@127.0.0.1:27017/app?authSource=admin",
	},
}

loader := parent.InitStarterLoader([]parent.Starter{starter})
if err := loader.Start(); err != nil {
	panic(err)
}
```

The default database may come from the URI path or `Database`; the explicit field takes precedence. Use `LazyConfig` to resolve configuration once at first use during startup or the default initialization hook. It takes precedence over `Config` when both are present. Stop the client through the parent loader so it participates in the shared shutdown lifecycle.

## Configuration

| Field | Purpose |
| --- | --- |
| `MongoURI` | MongoDB connection URI. |
| `Database` | Explicit default database. |
| `Pool` | Per-server connection pool overrides; nil fields preserve URI or driver defaults. |
| `BSONOptions` | Default BSON encoding and decoding behavior. |
| `EnableLogger` | Log command and database names at trace level. |
| `Compressors` | Network compression algorithms. |
| `InitFunc` | Callback invoked after startup with the initialized client. |

A `nil` `Compressors` value enables the starter defaults: `zstd`, `zlib`, and `snappy`. An empty slice disables compression. This field overrides compression settings in the URI. Configuration is cached once; compressors, BSON options, and pool pointer values are copied, and later changes to input configuration do not reconfigure the client.

```go
starter := &mongostarter.MongoStarter{
	Config: mongostarter.MongoConfig{
		MongoURI:    "mongodb://127.0.0.1:27017",
		Database:    "app",
		Compressors: []string{"zstd"},
		BSONOptions: &options.BSONOptions{ObjectIDAsHexString: true},
		EnableLogger: true,
	},
}
```

Startup validates the URI and database, connects, and pings the primary server with a fixed five-second deadline before making the client available. The primary health-check deadline is separate from driver connection and operation timeouts. `InitFunc` runs through the default parent hook after successful startup; replacing `MongoSetting` replaces this hook too.

### Connection Pool and Timeouts

Use `Pool` for typed settings, or keep existing pool options in `MongoURI`. Precedence is applied per field: **non-nil Pool field > URI option > driver default**.

```go
maxPool, minPool, maxConnecting := uint64(50), uint64(5), uint64(2)
maxIdleTime := 5 * time.Minute
starter := &mongostarter.MongoStarter{
    Config: mongostarter.MongoConfig{
        MongoURI: "mongodb://127.0.0.1:27017/app?timeoutMS=10000",
        Pool: mongostarter.PoolConfig{
            MaxPoolSize:     &maxPool,
            MinPoolSize:     &minPool,
            MaxConnecting:  &maxConnecting,
            MaxConnIdleTime: &maxIdleTime,
        },
    },
}
```

| Pool field | URI option | Driver default | Explicit zero |
| --- | --- | --- | --- |
| `MaxPoolSize` | `maxPoolSize` | 100 | Unlimited connections per server pool |
| `MinPoolSize` | `minPoolSize` | 0 | No maintained minimum |
| `MaxConnecting` | `maxConnecting` | 2 | Use the driver default of 2 |
| `MaxConnIdleTime` | `maxIdleTimeMS` | 0 | No idle time limit |

Nil fields retain the URI setting or driver default. For example, with URI `maxPoolSize=100&minPoolSize=5`, setting only `Pool.MaxPoolSize` to 30 preserves the minimum of 5. Use a pointer to zero to clear a URI minimum or idle limit. The URI must still be valid; an override does not repair URI parsing errors.

A negative idle duration or a minimum exceeding the effective nonzero maximum returns `ErrInvalidPoolConfig` before connecting. This includes the default maximum of 100 when no maximum is specified. Counts use `uint64`. Limits apply per server pool, not to all client connections combined. There is no SQL-style maximum idle connection count or connection total-lifetime field.

Driver timeouts remain URI options: `connectTimeoutMS` defaults to 30000 (zero disables it), `serverSelectionTimeoutMS` defaults to 30000, and `timeoutMS` is unset by default. These are driver `v2.9.2` defaults. Pool settings and longer driver timeouts do not extend the starter's fixed five-second startup Ping deadline. `InitFunc` receives an already initialized client and cannot change pool initialization options; configure `Pool` before startup.

## Model and Mapper

A model declares its collection name. Embedding `BaseMapper[T]` exposes the complete Mapper API without a constructor:

```go
type User struct {
	ID        string                 `bson:"_id,omitempty" json:"id"`
	Name      string                 `bson:"name" json:"name"`
	Status    string                 `bson:"status" json:"status"`
	CreatedAt mongostarter.Timestamp `bson:"createdAt" json:"createdAt"`
}

func (User) CollectionName() string { return "users" }

type UserMapper struct {
	mongostarter.BaseMapper[User]
}
```

The focused interfaces `QueryMapper`, `InsertMapper`, `UpdateMapper`, and `DeleteMapper` are aggregated by `Mapper[T]`. `RawMapper` provides access to the model collection.

## Query

Queries support three styles:

- `ByCond`: typed model condition.
- `ByBSON`: `bson.M` condition for MongoDB operators and explicit values.
- `WithOptions`: arbitrary driver filter plus native MongoDB options.

```go
mapper := UserMapper{}
var users []*User

err := mapper.SelectByCond(
	mongostarter.NewCondQuery(User{Status: "active"}).
		WithOrderBy(mongostarter.OrderBy{Column: "createdAt", Desc: true}).
		Select("name", "status").
		WithLimit(20),
	&users,
)
```

Complex BSON and native options remain available:

```go
err := mapper.SelectByBSON(
	mongostarter.NewBSONQuery(bson.M{"status": "active", "age": bson.M{"$gte": 18}}).
		WithOrderBy(
			mongostarter.OrderBy{Column: "status"},
			mongostarter.OrderBy{Column: "createdAt", Desc: true},
		),
	&users,
)

err = mapper.SelectWithOptions(
	bson.M{"status": "active"},
	&users,
	options.Find().SetLimit(20),
)
```

Use `SelectOne...` for one document and `Count...` for counts. A missing single result returns `mongo.ErrNoDocuments`; list operations decode into the supplied destination. Keep destination pointers nonnil and avoid sharing mutable result buffers between concurrent operations. Typed conditions use value semantics: `T` is the condition, `*T` is a single-result destination, and `*[]*T` is a list destination.

Count operations use the same condition query structures as document queries but ignore their projection, sorting, and list limit:

```go
total, err := mapper.CountByBSON(
	mongostarter.BSONQuery{Condition: bson.M{"status": "active"}},
)
```

`SelectColumns` creates an inclusion projection. MongoDB's `_id` is excluded unless explicitly selected.

`QueryOptions.Limit` applies only to list queries. Zero leaves the result unrestricted; negative values return `ErrInvalidQueryRange`. Pagination uses only `PageOptions.Number/Size`.

## ID Handling

String IDs are interpreted as hexadecimal MongoDB `ObjectID` values by default:

```go
var user User
err := mapper.SelectByID("507f1f77bcf86cd799439011", &user)
```

Pass `true` as the final argument for ordinary string IDs:

```go
err := mapper.SelectByID("user-1001", &user, true)
modified, err := mapper.UpdateByID(&User{Status: "active"}, "user-1001", true)
deleted, err := mapper.DeleteByID("user-1001", true)
```

The same behavior applies to batch ID queries and BSON-based ID updates. Non-string IDs are passed directly to the driver.

## Insert and Update

```go
id, err := mapper.Insert(&User{Name: "Alice", Status: "active"})

ids, err := mapper.InsertBatch([]*User{
	{Name: "Alice"},
	{Name: "Bob"},
})
```

`InsertWithBSON`, `InsertWithOptions`, and their batch variants support dynamic documents and native options.

Update values always precede conditions. Typed, BSON, and native-option variants are available:

```go
modified, err := mapper.UpdateByCond(
	&User{Status: "disabled"},
	User{Status: "inactive"},
)

modified, err = mapper.UpdateOneByBSON(
	bson.M{"$set": bson.M{"status": "active"}},
	bson.M{"name": "Alice"},
)
```

Use BSON updates when MongoDB operators such as `$set`, `$inc`, or array operators are required. Update APIs return MongoDB's modified document count.

BSON update methods accept both plain field documents and native update operators. Plain fields are wrapped in `$set`; documents whose top-level keys use MongoDB operators are passed through unchanged.

## Delete

```go
deleted, err := mapper.DeleteOneByBSON(bson.M{"name": "Alice"})
deleted, err = mapper.DeleteByCond(User{Status: "disabled"})
deleted, err = mapper.DeleteByID("507f1f77bcf86cd799439011")
```

Delete APIs return MongoDB's deleted document count. Condition-based updates and deletes reject filters considered empty by their guard with `ErrEmptyCondition`. Typed and native filter guards currently use default BSON encoding rather than configured `BSONOptions`. Options such as `OmitEmpty` or `UseJSONStructTags` can therefore change the actual filter after it passes the check. Do not rely on the guard alone: verify that the encoded filter is selective, or use an explicitly constructed BSON filter.

## Pagination

Page numbers start at `1`; page number and page size must both be positive.

```go
query := mongostarter.NewBSONPageQuery(bson.M{"status": "active"}, 2, 20).
	WithOrderBy(
			mongostarter.OrderBy{Column: "createdAt", Desc: true},
			mongostarter.OrderBy{Column: "name"},
		).
	Select("name", "status", "createdAt").
	WithFindOptions(options.Find().SetAllowDiskUse(true)).
	WithCountOptions(options.Count().SetHint("status_1"))

var users []*User
total, err := mapper.SelectPageByBSON(query, &users)
```

Pagination variants include `SelectPageByCond`, `SelectPageByBSON`, and `SelectPageWithOptions`. Count and find run separately and can observe different data. Explicit page settings override native skip/limit options. Do not concurrently reuse pagination queries sharing `FindOptions` storage; option builders and BSON maps must also remain stable while used.

## Raw Driver Access

Use raw accessors for operations outside `BaseMapper` or when an application-controlled `context.Context` is required:

```go
client := mongostarter.RawMongoClient()
database := mongostarter.RawDatabase()
otherDatabase := mongostarter.RawDatabase("analytics")
collection := mongostarter.RawCollection("users")
```

Raw accessors return `nil` before startup and after shutdown withdraws the client. Callers must not disconnect the shared client. `RawDatabase()` selects the default database; `RawDatabase("")` passes an empty database name rather than selecting the default. Only the first optional database argument is used. `mapper.Collection()` follows the same behavior and resolves the collection name from the model.

## Timestamp

`mongostarter.Timestamp` stores BSON dates at millisecond precision and serializes JSON as Unix timestamps using the toolkit global seconds/milliseconds setting. Zero time is stored as BSON `null` and JSON numeric zero; BSON or JSON null resets the value to Go's zero `time.Time`. JSON numeric zero instead decodes to the Unix epoch.

## Safety and Errors

- Filters considered empty by the mutation guard return `ErrEmptyCondition`; custom BSON encoding can bypass this protection as described above.
- Empty ID batches return `ErrEmptyIDs`.
- Invalid pagination returns `ErrInvalidPage`.
- Empty or nil sorting rules return `ErrInvalidOrderBy`.
- Unacknowledged writes return `ErrNotAcknowledged`.
- Missing or invalid connection settings return `ErrMongoURIRequired`, `ErrMongoDatabaseRequired`, or `ErrInvalidMongoURI`.
- Using the Mapper outside the active lifecycle returns `ErrMongoStarterNotStarted`.
- Shutdown timeouts return `ErrMongoStopTimeout`.

Exported package errors support `errors.Is`.

## Design Notes

- The package owns one process-wide client and one default database.
- Multiple active `MongoStarter` instances are not supported.
- Configuration is resolved once and retained for the starter lifecycle.
- Mapper operations use background contexts. URI `timeoutMS` can bound driver operations, but request cancellation and transaction sessions are not inherited; use raw collections when those contexts are required.
- Query builders copy sorting, projection, and option slices where documented; filter maps and option implementations remain borrowed. Keep mutable inputs stable during use.
- Batch insertion can partially succeed before a driver error; helpers return no IDs on error and do not roll back inserted documents.
- Shutdown withdraws the client before disconnecting. A timeout returns `gracefully=false`, `stopped=true`, and `ErrMongoStopTimeout`; retained native handles do not bypass shutdown.
- Command logging records command and database names without full payloads.
- A successfully stopped MongoDB starter is not restartable through the parent lifecycle.

## Testing

Default tests are deterministic package-level unit tests and do not require MongoDB:

```bash
go test ./...
```

The tests under `test/` use a real MongoDB deployment and are guarded by the `integration` build tag. Set `MONGO_TEST_URI` before running them explicitly:

```bash
MONGO_TEST_URI='mongodb://YOUR_USERNAME:YOUR_PASSWORD@127.0.0.1:27017/app' go test -tags=integration ./test
```

### Connection Pool Integration Tests

Set `MONGO_TEST_URI` to a reachable single-server MongoDB URI with appropriate authentication and, when needed, `directConnection=true`. Then run:

```bash
go test -mod=readonly -tags=integration -race -timeout=90s -count=1 -v ./mongostarter -run '^TestPoolIntegration'
```

These tests require the URI and fail when it is absent. They issue only Ping commands and verify pool capacity waits, minimum size, concurrent connection establishment, idle expiration, explicit zero overrides, and starter startup/shutdown. They do not execute the separate Mapper integration tests or their data cleanup.
