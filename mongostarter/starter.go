package mongostarter

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/acexy/golang-toolkit/logger"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

var (
	mongoClient     *mongo.Client
	defaultDatabase string
	mongoLock       sync.RWMutex
)

type MongoConfig struct {

	// 工作数据库
	Database string
	// Mongo 链接地址串 如果设置了则忽略上面的配置
	MongoURI string
	// 设置默认的 BSON 选项
	BSONOptions *options.BSONOptions
	// 开启详细日志
	EnableLogger bool
	// 网络压缩算法
	Compressors []string

	InitFunc func(instance *mongo.Client)
}

type MongoStarter struct {
	Config     MongoConfig
	LazyConfig func() MongoConfig

	config       *MongoConfig
	MongoSetting *parent.Setting
}

func (m *MongoStarter) getConfig() *MongoConfig {
	if m.config == nil {
		var config MongoConfig
		if m.LazyConfig != nil {
			config = m.LazyConfig()
		} else {
			config = m.Config
		}
		m.config = &config
	}
	return m.config
}

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

func (m *MongoStarter) Start() (any, error) {
	mongoLock.Lock()
	defer mongoLock.Unlock()
	if mongoClient != nil {
		return nil, ErrMongoStarterAlreadyStarted
	}

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
		// 从 URI 的路径部分提取数据库名称（如果有）
		parsedURI, err := url.Parse(clientOptions.GetURI())
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidMongoURI, err)
		}
		// 获取路径部分并去掉前导的 '/'
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
	mongoClient = client
	defaultDatabase = database
	return mongoClient, nil
}

func (m *MongoStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	mongoLock.Lock()
	defer mongoLock.Unlock()
	if mongoClient == nil {
		return false, true, ErrMongoStarterNotStarted
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()
	if err = mongoClient.Disconnect(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return false, false, ErrMongoStopTimeout
		}
		return false, false, err
	}
	mongoClient = nil
	defaultDatabase = ""
	return true, true, nil
}

// RawMongoClient 获取原始的 mongo.Client原生能力
func RawMongoClient() *mongo.Client {
	mongoLock.RLock()
	defer mongoLock.RUnlock()
	return mongoClient
}

// RawDatabase 获取原始的 mongo.Database 原生能力
// database 为空则使用默认初始化指定的database
func RawDatabase(database ...string) *mongo.Database {
	mongoLock.RLock()
	defer mongoLock.RUnlock()
	if mongoClient == nil {
		return nil
	}
	var db string
	if len(database) > 0 {
		db = database[0]
	} else {
		db = defaultDatabase
	}
	return mongoClient.Database(db)
}

// RawCollection 获取原始的 mongo.Collection 原生能力
func RawCollection(collection string, database ...string) *mongo.Collection {
	db := RawDatabase(database...)
	if db == nil {
		return nil
	}
	return db.Collection(collection)
}
