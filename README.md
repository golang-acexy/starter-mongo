# starter-mongo

`starter-mongo` brings the official MongoDB Go driver into the golang-acexy starter lifecycle. It manages one application-wide client and provides generic Mapper APIs for common document operations.

## Highlights

- One lifecycle-managed MongoDB client and default database.
- `BaseMapper[T]` covers query, insert, update, delete, count, and pagination operations.
- Typed models, `bson.M`, and native driver filters/options can be used side by side.
- String IDs support both MongoDB `ObjectID` and ordinary string values.
- Multi-field sorting, projection, and native find/count options are built into pagination.
- Empty-condition updates and deletes are rejected before reaching MongoDB.
- Write APIs return acknowledged insert IDs or affected document counts.
- Narrow raw accessors remain available for advanced driver operations.

`cloud-database/mongo` builds business-oriented Repository APIs on top of this module; client lifecycle remains owned by `starter-mongo`.

## Requirements

- Go `1.25.8`

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

The default database may come from the URI path or `Database`; the explicit field takes precedence. Use `LazyConfig` to resolve configuration immediately before startup. It takes precedence over `Config` when both are present. Stop the client through the parent loader so it participates in the shared shutdown lifecycle.

## Configuration

| Field | Purpose |
| --- | --- |
| `MongoURI` | MongoDB connection URI. |
| `Database` | Explicit default database. |
| `BSONOptions` | Default BSON encoding and decoding behavior. |
| `EnableLogger` | Log command and database names at trace level. |
| `Compressors` | Network compression algorithms. |
| `InitFunc` | Callback invoked after startup with the initialized client. |

A `nil` `Compressors` value enables the starter defaults: `zstd`, `zlib`, and `snappy`. An empty slice disables those defaults.

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

Startup validates the URI and database, connects, and pings the primary server before publishing the client.

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

Use `SelectOne...` for one document and `Count...` for counts. Typed conditions use value semantics: `T` is the condition, `*T` is a single-result destination, and `*[]*T` is a list destination.

Count operations use the same condition query structures as document queries:

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

## Delete

```go
deleted, err := mapper.DeleteOneByBSON(bson.M{"name": "Alice"})
deleted, err = mapper.DeleteByCond(User{Status: "disabled"})
deleted, err = mapper.DeleteByID("507f1f77bcf86cd799439011")
```

Delete APIs return MongoDB's deleted document count. Condition-based updates and deletes reject empty conditions with `ErrEmptyCondition`.

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

Pagination variants include `SelectPageByCond`, `SelectPageByBSON`, and `SelectPageWithOptions`.

## Raw Driver Access

Use raw accessors for operations outside `BaseMapper` or when an application-controlled `context.Context` is required:

```go
client := mongostarter.RawMongoClient()
database := mongostarter.RawDatabase()
otherDatabase := mongostarter.RawDatabase("analytics")
collection := mongostarter.RawCollection("users")
```

Raw accessors return `nil` before startup. `mapper.Collection()` follows the same behavior and resolves the collection name from the model.

## Timestamp

`mongostarter.Timestamp` stores BSON dates and serializes JSON as Unix timestamps. A zero timestamp is stored as BSON `null` and decoded as Go's zero `time.Time`.

## Safety and Errors

- Empty update or delete conditions return `ErrEmptyCondition`.
- Empty ID batches return `ErrEmptyIDs`.
- Invalid pagination returns `ErrInvalidPage`.
- Unacknowledged writes return `ErrNotAcknowledged`.
- Missing or invalid connection settings return `ErrMongoURIRequired`, `ErrMongoDatabaseRequired`, or `ErrInvalidMongoURI`.
- Using the Mapper outside the active lifecycle returns `ErrMongoStarterNotStarted`.
- Shutdown timeouts return `ErrMongoStopTimeout`.

Exported package errors support `errors.Is`.

## Design Notes

- The package owns one process-wide client and one default database.
- Multiple active `MongoStarter` instances are not supported.
- Configuration is resolved once and retained for the starter lifecycle.
- Shutdown disconnects the client and clears package-owned runtime state.
- Command logging records command and database names without full payloads.
- A successfully stopped MongoDB starter is not restartable through the parent lifecycle.
