package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-mongo/mongostarter"
	"go.mongodb.org/mongo-driver/v2/bson"
	"testing"
)

func (StartupLog) CollectionName() string {
	return "startup_log"
}

type StartupLogMapper struct {
	mongostarter.BaseMapper[StartupLog]
}

var mapper = StartupLogMapper{}

func TestSelectById(t *testing.T) {
	var log StartupLog
	_ = mapper.SelectById("998a29f641e6-1726056851285", &log)
	fmt.Println(json.ToJson(log))
}

func TestSelectByIds(t *testing.T) {
	var logs []StartupLog
	_ = mapper.SelectByIds([]string{"998a29f641e6-1726056851285", "998a29f641e6-1726056854030"}, &logs)
	fmt.Println(json.ToJson(logs))
}

func TestSelectOne(t *testing.T) {
	data := StartupLog{Pid: 28}
	err := mapper.SelectOneByCondition(&data, &data)
	fmt.Println(err)
	fmt.Println(json.ToJson(data))

	err = mapper.SelectOneByCondition(&data, &data, "hostname")
	fmt.Println(err)
	fmt.Println(json.ToJson(data))

	err = mapper.SelectOneByBson(bson.M{"pid": 28}, &data)
	fmt.Println(err)
	fmt.Println(json.ToJson(data))
}

func TestSelect(t *testing.T) {
	data := StartupLog{Hostname: "998a29f641e6"}
	var dataList []StartupLog
	err := mapper.SelectByCondition(&data, &dataList)
	fmt.Println(err)
	fmt.Println(json.ToJson(dataList))

	err = mapper.SelectByBson(bson.M{"hostname": "998a29f641e6"}, &dataList)
	fmt.Println(err)
	fmt.Println(json.ToJson(dataList))
}
