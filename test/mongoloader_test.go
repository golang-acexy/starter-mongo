package test

import (
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-mongo/mongostarter"
	"github.com/golang-acexy/starter-parent/parent"
	"testing"
	"time"
)

func TestLoader(t *testing.T) {
	loader := parent.NewStarterLoader([]parent.Starter{
		&mongostarter.MongoStarter{
			MongoUri: "mongodb://acexy:tech-acexy@localhost:27017",
		},
	})

	loader.Start()
	time.Sleep(time.Second * 5)
	result, _ := loader.StopBySetting()
	println(json.ToJsonFormat(result))
}
