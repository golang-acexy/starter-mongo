# starter-mongo

`starter-mongo` integrates the official MongoDB Go driver with the shared golang-acexy starter lifecycle. It manages one application-wide MongoDB client and provides a generic `BaseMapper` for common query, insert, update, delete, count, and pagination operations.

## Requirements

Current module Go version: `1.25.8`.

```bash
go get github.com/golang-acexy/starter-mongo
```

## Starter Usage

Register one `MongoStarter` with the parent loader:

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

The default database may be supplied through the URI path or the `Database` field. At least one of them must define a database name.

Use `LazyConfig` when configuration must be resolved immediately before startup. When both `Config` and `LazyConfig` are provided, `LazyConfig` takes precedence.

```go
starter := &mongostarter.MongoStarter{
	LazyConfig: func() mongostarter.MongoConfig {
		return loadMongoConfig()
	},
}
```

Stop MongoDB through the parent loader so it participates in the shared shutdown lifecycle:

```go
_, err := loader.StopAllByRegisteredOrder(10 * time.Second)
if err != nil {
	panic(err)
}
```

## Configuration

`MongoConfig` supports the following fields:

| Field | Description |
| --- | --- |
| `MongoURI` | Required MongoDB connection URI. |
| `Database` | Default database. It overrides the database name in the URI. |
| `BSONOptions` | Default BSON encoding and decoding behavior passed to the MongoDB client. |
| `EnableLogger` | Logs MongoDB command names and database names at trace level. |
| `Compressors` | Network compressors. A `nil` value uses `zstd`, `zlib`, and `snappy`; an empty slice disables the starter defaults. |
| `InitFunc` | Callback invoked after startup with the initialized `*mongo.Client`. |

Example with explicit client options:

```go
starter := &mongostarter.MongoStarter{
	Config: mongostarter.MongoConfig{
		MongoURI:    "mongodb://127.0.0.1:27017",
		Database:    "app",
		Compressors: []string{"zstd"},
		BSONOptions: &options.BSONOptions{
			ObjectIDAsHexString: true,
		},
		EnableLogger: true,
		InitFunc: func(client *mongo.Client) {
			// Perform application-specific initialization here.
		},
	},
}
```

## Model and Mapper

A model only needs to declare its MongoDB collection name. Embed `BaseMapper[T]` in a model-specific mapper to obtain the complete mapper API.

```go
type User struct {
	ID        string                 `bson:"_id,omitempty" json:"id"`
	Name      string                 `bson:"name" json:"name"`
	Status    string                 `bson:"status" json:"status"`
	CreatedAt mongostarter.Timestamp `bson:"createdAt" json:"createdAt"`
}

func (User) CollectionName() string {
	return "users"
}

type UserMapper struct {
	mongostarter.BaseMapper[User]
}
```

No constructor or interface assertion is required. Methods embedded from `BaseMapper[User]` are promoted automatically to `UserMapper`.

The mapper API is split into focused interfaces and aggregated by `Mapper[T]`:

- `RawMapper` provides access to the model's collection.
- `QueryMapper[T]` provides query, count, and pagination operations.
- `InsertMapper[T]` provides single and batch insert operations.
- `UpdateMapper[T]` provides single and multi-document update operations.
- `DeleteMapper[T]` provides single and multi-document delete operations.
- `Mapper[T]` combines all capabilities above.

## ID Handling

String IDs are treated as hexadecimal MongoDB `ObjectID` values by default:

```go
var user User
err := mapper.SelectByID("507f1f77bcf86cd799439011", &user)
```

Pass `true` as the final argument when the collection uses ordinary string IDs:

```go
err := mapper.SelectByID("user-1001", &user, true)

count, err := mapper.UpdateByID(
	&User{Status: "active"},
	"user-1001",
	true,
)

count, err = mapper.DeleteByID("user-1001", true)
```

The same rule applies to `SelectByIDs`, `UpdateByIDWithBSON`, and `DeleteByID`. Non-string ID values are passed directly to the MongoDB driver.

## Query Operations

Mapper queries are available in three forms:

- `ByCond` accepts a typed model as the filter.
- `ByBSON` accepts a `bson.M` filter.
- `WithOptions` accepts a driver filter and native MongoDB options.

Typed condition query:

```go
condition := &User{Status: "active"}
var users []*User

err := mapper.SelectByCond(
	condition,
	mongostarter.NewOrderBy("createdAt", true),
	&users,
	"name",
	"status",
)
```

BSON query:

```go
var users []*User
err := mapper.SelectByBSON(
	bson.M{"status": "active", "age": bson.M{"$gte": 18}},
	mongostarter.NewOrderBys(
		mongostarter.OrderBy{Column: "status"},
		mongostarter.OrderBy{Column: "createdAt", Desc: true},
	),
	&users,
)
```

Native options query:

```go
var users []*User
err := mapper.SelectWithOptions(
	bson.M{"status": "active"},
	&users,
	options.Find().SetLimit(20).SetProjection(bson.M{"name": 1}),
)
```

Use `SelectOneByCond`, `SelectOneByBSON`, or `SelectOneWithOptions` when one document is expected. Use `CountByCond`, `CountByBSON`, or `CountWithOptions` for counts.

The `specifyColumns` arguments build an inclusion projection. MongoDB's `_id` field is excluded unless `_id` is explicitly requested.

## Insert Operations

Insert a typed model:

```go
id, err := mapper.Insert(&User{
	Name:   "Alice",
	Status: "active",
})
```

Insert a BSON document or use native insert options:

```go
id, err := mapper.InsertWithBSON(bson.M{
	"name":   "Alice",
	"status": "active",
})

id, err = mapper.InsertWithOptions(
	bson.M{"name": "Bob"},
	options.InsertOne(),
)
```

Batch variants return the inserted IDs in input order:

```go
ids, err := mapper.InsertBatch([]*User{
	{Name: "Alice"},
	{Name: "Bob"},
})
```

`InsertBatchWithBSON` and `InsertBatchWithOptions` are available for BSON documents and native driver options.

## Update Operations

All update methods return MongoDB's modified document count. Update arguments always come before condition arguments.

```go
modified, err := mapper.UpdateOneByBSON(
	bson.M{"$set": bson.M{"status": "active"}},
	bson.M{"name": "Alice"},
)

modified, err = mapper.UpdateByBSON(
	bson.M{"$set": bson.M{"status": "disabled"}},
	bson.M{"status": "inactive"},
)
```

Typed alternatives are `UpdateByID`, `UpdateOneByCond`, and `UpdateByCond`. Use `UpdateByIDWithBSON` when the update requires MongoDB operators.

All condition-based update methods reject empty conditions with `ErrEmptyCondition`. This prevents an accidental update of an entire collection.

## Delete Operations

All delete methods return MongoDB's deleted document count.

```go
deleted, err := mapper.DeleteOneByBSON(bson.M{"name": "Alice"})

deleted, err = mapper.DeleteByBSON(bson.M{"status": "disabled"})
```

Typed alternatives are `DeleteOneByCond` and `DeleteByCond`. Use `DeleteByID` for a single document identified by `_id`.

Condition-based delete methods reject empty conditions with `ErrEmptyCondition`. This prevents an accidental deletion of an entire collection.

## Pagination

`PageQuery` combines pagination, sorting, projection, and native find/count options:

```go
query := mongostarter.PageQuery{
	PageNumber: 2,
	PageSize:   20,
	OrderBy: mongostarter.NewOrderBys(
		mongostarter.OrderBy{Column: "createdAt", Desc: true},
		mongostarter.OrderBy{Column: "name"},
	),
	SpecifyColumns: []string{"name", "status", "createdAt"},
	FindOptions: []options.Lister[options.FindOptions]{
		options.Find().SetAllowDiskUse(true),
	},
	CountOptions: []options.Lister[options.CountOptions]{
		options.Count().SetHint("status_1"),
	},
}

var users []*User
total, err := mapper.SelectPageByBSON(
	bson.M{"status": "active"},
	query,
	&users,
)
```

`PageNumber` is one-based. Both `PageNumber` and `PageSize` must be greater than zero; invalid values return `ErrInvalidPage`.

The available pagination methods are:

- `SelectPageByCond` for typed model conditions.
- `SelectPageByBSON` for BSON conditions.
- `SelectPageWithOptions` for arbitrary driver filters.

## Raw Driver Access

Use the narrow raw accessors when an operation is not covered by `BaseMapper`:

```go
client := mongostarter.RawMongoClient()
database := mongostarter.RawDatabase()
otherDatabase := mongostarter.RawDatabase("analytics")
collection := mongostarter.RawCollection("users")
```

The mapper-specific accessor validates starter state and resolves the model's collection:

```go
collection, err := mapper.Collection()
```

Package-level raw accessors return `nil` before successful startup. Prefer `Collection()` when explicit startup errors are useful.

Raw driver access is also appropriate when an operation requires a caller-controlled `context.Context`; the current `BaseMapper` methods manage their own background contexts.

## Timestamp

`mongostarter.Timestamp` stores MongoDB values as BSON dates and serializes JSON values as Unix timestamps.

```go
type AuditLog struct {
	CreatedAt mongostarter.Timestamp `bson:"createdAt" json:"createdAt"`
}
```

A zero timestamp is stored as BSON `null` and restored as Go's zero `time.Time` value.

## Common Errors

Use `errors.Is` to inspect exported package errors:

```go
if errors.Is(err, mongostarter.ErrEmptyCondition) {
	// Reject or correct the request.
}
```

Important errors include:

| Error | Meaning |
| --- | --- |
| `ErrMongoStarterAlreadyStarted` | Another Mongo starter has already initialized the global client. |
| `ErrMongoStarterNotStarted` | A mapper or lifecycle operation was used before startup. |
| `ErrMongoStopTimeout` | Client shutdown exceeded the supplied timeout. |
| `ErrMongoURIRequired` | `MongoURI` is empty. |
| `ErrMongoDatabaseRequired` | Neither `Database` nor the URI defines a default database. |
| `ErrInvalidMongoURI` | The MongoDB URI could not be parsed. |
| `ErrEmptyIDs` | `SelectByIDs` received an empty ID list. |
| `ErrEmptyCondition` | A protected update or delete operation received an empty condition. |
| `ErrInvalidPage` | Pagination parameters are not greater than zero. |
| `ErrNotAcknowledged` | MongoDB did not acknowledge a write operation. |

## Design Notes

- The package owns one process-wide MongoDB client and one default database.
- Registering or starting multiple `MongoStarter` instances is not supported.
- Configuration is resolved once per starter instance and retained for its lifecycle.
- Startup validates the URI and database, connects, and pings the primary server before publishing the global client.
- Shutdown disconnects the client and clears package-owned runtime state.
- Mapper write methods report acknowledged MongoDB result counts rather than inferring success from the absence of an error.
- Command logging records command and database names without logging full command payloads.
