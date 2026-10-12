package mongostarter

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acexy/golang-toolkit/logger"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var (
	mongoRuntimeState atomic.Pointer[mongoRuntime]
	mongoLock         sync.Mutex
	mongoState        lifecycleState
)

type mongoRuntime struct {
	client   *mongo.Client
	database string
}

type lifecycleState uint8

const (
	lifecycleStopped lifecycleState = iota
	lifecycleStarting
	lifecycleRunning
	lifecycleStopping
)

// MongoConfig configures the singleton client; connection pool and driver timeouts are configured through MongoURI.
type MongoConfig struct {

	// Database selects the default database; nonempty values override the URI path.
	Database string
	// MongoURI is required and configures driver options, including pool sizes and timeouts.
	MongoURI string
	// BSONOptions configures driver encoding/decoding; empty-filter guards use default encoding instead.
	BSONOptions *options.BSONOptions
	// EnableLogger logs command and database names at trace level without command payloads.
	EnableLogger bool
	// Compressors overrides URI compression; nil selects zstd/zlib/snappy and an empty slice disables compression.
	Compressors []string

	// InitFunc is invoked by the default parent hook after startup; it cannot change connection initialization options.
	InitFunc func(instance *mongo.Client)
}

// MongoStarter owns the shared client lifecycle; configure fields before startup and keep them fixed while running.
type MongoStarter struct {
	// Config supplies static configuration when LazyConfig is nil.
	Config     MongoConfig
	// LazyConfig takes precedence and is evaluated once on first resolution during startup or the init hook.
	LazyConfig func() MongoConfig

	config       *MongoConfig
	configOnce   sync.Once
	// MongoSetting overrides the default parent setting, including the InitFunc hook.
	MongoSetting *parent.Setting
}

func (m *MongoStarter) getConfig() *MongoConfig {
	m.configOnce.Do(func() {
		var config MongoConfig
		if m.LazyConfig != nil {
			config = m.LazyConfig()
		} else {
			config = m.Config
		}
		config.Compressors = slices.Clone(config.Compressors)
		if config.BSONOptions != nil {
			bsonOptions := *config.BSONOptions
			config.BSONOptions = &bsonOptions
		}
		m.config = &config
	})
	return m.config
}

// Setting returns a custom setting or the default non-restartable parent setting with a post-start InitFunc hook.
func (m *MongoStarter) Setting() *parent.Setting {
	if m.MongoSetting != nil {
		return m.MongoSetting
	}
	return parent.NewSetting(
		"Mongo-Starter",
		false,
		21,
		true,
		time.Second*30,
		func(instance any) {
			config := m.getConfig()
			if config.InitFunc != nil {
				config.InitFunc(instance.(*mongo.Client))
			}
		})
}

// Start connects and verifies primary availability with a fixed five-second Ping deadline.
// It publishes the client only on success; overlapping lifecycle transitions return ErrMongoStarterAlreadyStarted.
func (m *MongoStarter) Start() (any, error) {
	mongoLock.Lock()
	if mongoState != lifecycleStopped {
		mongoLock.Unlock()
		return nil, ErrMongoStarterAlreadyStarted
	}
	mongoState = lifecycleStarting
	mongoLock.Unlock()

	started := false
	defer func() {
		if !started {
			mongoLock.Lock()
			mongoState = lifecycleStopped
			mongoLock.Unlock()
		}
	}()

	config := m.getConfig()
	if strings.TrimSpace(config.MongoURI) == "" {
		return nil, ErrMongoURIRequired
	}
	database := config.Database
	clientOptions := options.Client().ApplyURI(config.MongoURI)
	if config.EnableLogger {
		monitor := &event.CommandMonitor{
			Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
				logger.Logrus().WithField("database", evt.DatabaseName).Traceln(evt.CommandName)
			},
		}
		clientOptions.SetMonitor(monitor)
	}
	if config.Compressors != nil {
		clientOptions.SetCompressors(config.Compressors)
	} else {
		clientOptions.SetCompressors([]string{"zstd", "zlib", "snappy"})
	}
	if config.BSONOptions != nil {
		clientOptions.SetBSONOptions(config.BSONOptions)
	}
	if database == "" {
		// Resolve the URI database only when the explicit database is absent.
		parsedURI, err := url.Parse(clientOptions.GetURI())
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidMongoURI, err)
		}
		// Remove the leading slash from the URI database path.
		uriDatabase := strings.TrimPrefix(parsedURI.Path, "/")
		if uriDatabase != "" {
			database = uriDatabase
		}
	}
	if database == "" {
		return nil, ErrMongoDatabaseRequired
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	mongoLock.Lock()
	mongoRuntimeState.Store(&mongoRuntime{client: client, database: database})
	mongoState = lifecycleRunning
	mongoLock.Unlock()
	started = true
	return client, nil
}

// Stop withdraws shared handles before disconnecting within maxWaitTime; callbacks receive no cancellation signal.
// Timeout returns false, true, ErrMongoStopTimeout. Other errors also leave the runtime withdrawn.
// Nonpositive waits create an expired deadline; raw handles must not be used to bypass shutdown.
func (m *MongoStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	mongoLock.Lock()
	runtime := mongoRuntimeState.Load()
	if mongoState != lifecycleRunning || runtime == nil {
		mongoLock.Unlock()
		return false, true, ErrMongoStarterNotStarted
	}
	client := runtime.client
	mongoRuntimeState.Store(nil)
	mongoState = lifecycleStopping
	mongoLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()
	err = client.Disconnect(ctx)

	mongoLock.Lock()
	mongoState = lifecycleStopped
	mongoLock.Unlock()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return false, true, ErrMongoStopTimeout
		}
		return false, true, err
	}
	return true, true, nil
}

// RawMongoClient returns the borrowed shared client, or nil outside the active lifecycle; callers must not disconnect it.
func RawMongoClient() *mongo.Client {
	runtime := mongoRuntimeState.Load()
	if runtime == nil {
		return nil
	}
	return runtime.client
}

// RawDatabase returns a borrowed database handle, or nil outside the active lifecycle.
// With no argument it uses the startup database; otherwise only the first argument is used, even when empty.
func RawDatabase(database ...string) *mongo.Database {
	runtime := mongoRuntimeState.Load()
	if runtime == nil {
		return nil
	}
	var db string
	if len(database) > 0 {
		db = database[0]
	} else {
		db = runtime.database
	}
	return runtime.client.Database(db)
}

// RawCollection returns a borrowed collection handle, or nil outside the active lifecycle.
// The optional database follows RawDatabase semantics; callers own custom context/session operations.
func RawCollection(collection string, database ...string) *mongo.Collection {
	db := RawDatabase(database...)
	if db == nil {
		return nil
	}
	return db.Collection(collection)
}
