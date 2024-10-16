package mongostarter

import (
	"context"
	"github.com/acexy/golang-toolkit/logger"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"net/url"
	"strings"
	"time"
)

var mongoClient *mongo.Client
var defaultDatabase string

type MongoStarter struct {

	// 工作数据库
	Database string

	// Mongo 链接地址串 如果设置了则忽略上面的配置
	MongoUri string

	// 设置默认的Bson选项
	BsonOpts *options.BSONOptions

	// 开启详细日志
	EnableLogger bool

	// 网络压缩算法
	Compressors []string

	MongoSetting *parent.Setting

	InitFunc func(instance *mongo.Client)
}

func (m *MongoStarter) Setting() *parent.Setting {
	if m.MongoSetting != nil {
		return m.MongoSetting
	}
	return parent.NewSetting(
		"Mongo-Starter",
		21,
		false,
		time.Second*30,
		func(instance interface{}) {
			if m.InitFunc != nil {
				m.InitFunc(instance.(*mongo.Client))
			}
		})
}

func (m *MongoStarter) Start() (interface{}, error) {
	if m.Database != "" {
		defaultDatabase = m.Database
	}
	clientOptions := options.Client().ApplyURI(m.MongoUri)
	if m.EnableLogger {
		monitor := &event.CommandMonitor{
			Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
				logger.Logrus().Traceln(evt.CommandName, evt.Command)
			},
		}
		clientOptions.SetMonitor(monitor)
	}
	if m.Compressors != nil {
		clientOptions.SetCompressors(m.Compressors)
	} else {
		clientOptions.SetCompressors([]string{"zstd", "zlib", "snappy"})
	}
	if m.BsonOpts != nil {
		clientOptions.SetBSONOptions(m.BsonOpts)
	}

	if defaultDatabase == "" {
		// 从 URI 的路径部分提取数据库名称（如果有）
		parsedURI, err := url.Parse(clientOptions.GetURI())
		if err == nil {
			// 获取路径部分并去掉前导的 '/'
			database := strings.TrimPrefix(parsedURI.Path, "/")
			if database != "" {
				defaultDatabase = database
			}
		}

	}

	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	mongoClient = client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return mongoClient, mongoClient.Ping(ctx, readpref.Primary())
}

func (m *MongoStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	done := make(chan error)
	go func() {
		done <- mongoClient.Disconnect(ctx)
	}()
	stopped = true
	select {
	case <-time.After(maxWaitTime):
		return
	case err = <-done:
		if err == nil {
			gracefully = true
		}
	}
	return
}

// RawMongoClient 获取原始的 mongo.Client原生能力
func RawMongoClient() *mongo.Client {
	return mongoClient
}

// RawDatabase 获取原始的 mongo.Database 原生能力
// database 为空则使用默认的 database
func RawDatabase(database ...string) *mongo.Database {
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
	return RawDatabase(database...).Collection(collection)
}
