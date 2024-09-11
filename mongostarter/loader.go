package mongostarter

import (
	"context"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

var mongoClient *mongo.Client

type MongoStarter struct {

	// Mongo 链接地址串
	MongoUri string

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
	clientOptions := options.Client().ApplyURI(m.MongoUri)
	if m.Compressors != nil {
		clientOptions.SetCompressors(m.Compressors)
	} else {
		clientOptions.SetCompressors([]string{"zstd", "zlib", "snappy"})
	}
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}
	mongoClient = client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return mongoClient, mongoClient.Ping(ctx, readpref.Primary())
}

func (m *MongoStarter) Stop(maxWaitTime time.Duration) (gracefully, stopped bool, err error) {
	done := make(chan error)
	go func() {
		err = mongoClient.Disconnect(context.Background())
		done <- err
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

func RawMongoClient() *mongo.Client {
	return mongoClient
}
