package mongostarter

import "errors"

var (
	// ErrInvalidPoolConfig reports an invalid idle duration or effective pool size relationship.
	ErrInvalidPoolConfig = errors.New("invalid mongo pool configuration")
	// ErrMongoStarterAlreadyStarted reports an existing client or a lifecycle transition in progress.
	ErrMongoStarterAlreadyStarted = errors.New("mongo starter already started")
	// ErrMongoStarterNotStarted reports that no runtime is available for the operation.
	ErrMongoStarterNotStarted = errors.New("mongo starter not started")
	// ErrMongoStopTimeout reports a disconnect deadline failure after runtime withdrawal.
	ErrMongoStopTimeout = errors.New("waiting for mongo starter shutdown timeout")
	// ErrEmptyIDs reports an empty ID batch.
	ErrEmptyIDs = errors.New("ids must not be empty")
	// ErrEmptyCondition reports a filter considered empty by the default-encoding guard.
	ErrEmptyCondition = errors.New("condition must not be empty")
	// ErrInvalidPage reports nonpositive page inputs or an overflowing offset.
	ErrInvalidPage = errors.New("page number and page size must be greater than zero")
	// ErrInvalidQueryRange reports a negative list limit.
	ErrInvalidQueryRange = errors.New("query limit must not be negative")
	// ErrInvalidOrderBy reports a nil sort entry or blank BSON column name.
	ErrInvalidOrderBy = errors.New("order by column must not be empty")
	// ErrNotAcknowledged reports a write result without acknowledgment; it does not undo database effects.
	ErrNotAcknowledged = errors.New("mongo operation was not acknowledged")
	// ErrMongoURIRequired reports a blank connection URI.
	ErrMongoURIRequired = errors.New("mongo URI is required")
	// ErrMongoDatabaseRequired reports the absence of both an explicit database and a URI database.
	ErrMongoDatabaseRequired = errors.New("mongo database is required")
	// ErrInvalidMongoURI wraps URI path parsing failures; other URI validation errors can come directly from the driver.
	ErrInvalidMongoURI = errors.New("invalid mongo URI")
)
