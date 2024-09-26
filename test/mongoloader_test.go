package test

import (
	"context"
	"fmt"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-mongo/mongostarter"
	"github.com/golang-acexy/starter-parent/parent"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"testing"
)

var loader *parent.StarterLoader

func init() {
	loader = parent.NewStarterLoader([]parent.Starter{
		&mongostarter.MongoStarter{
			MongoUri: "mongodb://acexy:tech-acexy@localhost:27017",
			Database: "local",
			BsonOpts: &options.BSONOptions{
				UseJSONStructTags:   true,
				ObjectIDAsHexString: true,
				OmitZeroStruct:      true,
				ZeroStructs:         true,
			},
			EnableLogger: true,
		},
	})
	err := loader.Start()
	if err != nil {
		println(err)
		return
	}
}

type StartupLog struct {
	ID       string `bson:"_id,omitempty" json:"id"`
	Pid      int    `bson:"pid,omitempty" json:"pid"`
	Hostname string `bson:"hostname,omitempty" json:"hostname"`
}

func TestLoader(t *testing.T) {
	logColl := mongostarter.RawCollection("startup_log")
	count, _ := logColl.CountDocuments(context.Background(), bson.D{})
	fmt.Println(count)

	var log StartupLog
	var logs []StartupLog
	_ = logColl.FindOne(context.Background(), bson.M{}).Decode(&log)
	fmt.Println(json.ToJsonFormat(log))

	cursor, _ := logColl.Find(context.Background(), bson.M{"_id": bson.M{"$in": []string{"998a29f641e6-1726056851285", "998a29f641e6-1726056854030"}}})
	_ = cursor.All(context.TODO(), &logs)
	fmt.Println(json.ToJson(logs))

	result, _ := loader.StopBySetting()
	println(json.ToJsonFormat(result))
}
