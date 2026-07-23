package mongostarter

import "errors"

var (
	ErrMongoStarterAlreadyStarted = errors.New("mongo starter already started")
	ErrMongoStarterNotStarted     = errors.New("mongo starter not started")
	ErrMongoStopTimeout           = errors.New("waiting for mongo starter shutdown timeout")
	ErrEmptyIDs                   = errors.New("ids must not be empty")
	ErrEmptyCondition             = errors.New("condition must not be empty")
	ErrInvalidPage                = errors.New("page number and page size must be greater than zero")
	ErrNotAcknowledged            = errors.New("mongo operation was not acknowledged")
	ErrMongoURIRequired           = errors.New("mongo URI is required")
	ErrMongoDatabaseRequired      = errors.New("mongo database is required")
	ErrInvalidMongoURI            = errors.New("invalid mongo URI")
)
