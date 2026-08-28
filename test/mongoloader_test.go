//go:build integration

package test

import (
	"context"
	"os"
	"testing"

	"github.com/golang-acexy/starter-mongo/mongostarter"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var loader *parent.StarterLoader

func TestMain(m *testing.M) {
	mongoURI := os.Getenv("MONGO_TEST_URI")
	if mongoURI == "" {
		os.Exit(0)
	}
	loader = parent.InitStarterLoader([]parent.Starter{
		&mongostarter.MongoStarter{
			Config: mongostarter.MongoConfig{
				MongoURI: mongoURI,
				//Database: "local",
				BSONOptions: &options.BSONOptions{
					UseJSONStructTags:   true,
					ObjectIDAsHexString: true,
					OmitZeroStruct:      true,
					ZeroStructs:         true,
				},
				EnableLogger: true,
			},
		},
	})
	if err := loader.Start(); err != nil {
		os.Exit(1)
	}
	code := m.Run()
	if collection := mongostarter.RawCollection(testCollection); collection != nil {
		_, _ = collection.DeleteMany(context.Background(), bson.M{})
	}
	_, _ = loader.StopAllBySetting()
	os.Exit(code)
}

func TestLoader(t *testing.T) {
	if mongostarter.RawMongoClient() == nil {
		t.Fatal("mongo client is not initialized")
	}
	if mongostarter.RawDatabase() == nil {
		t.Fatal("mongo database is not initialized")
	}
	logColl := mongostarter.RawCollection(testCollection)
	if logColl == nil {
		t.Fatal("mongo collection is not initialized")
	}
	if _, err := logColl.CountDocuments(context.Background(), bson.D{}); err != nil {
		t.Fatal(err)
	}
}
