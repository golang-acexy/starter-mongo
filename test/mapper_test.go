package test

import (
	"fmt"
	"github.com/acexy/golang-toolkit/util/json"
	"github.com/golang-acexy/starter-mongo/mongostarter"
	"go.mongodb.org/mongo-driver/v2/bson"
	"testing"
	"time"
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
	fmt.Println(mapper.SelectById("123456", &log, true))
	fmt.Println(json.ToJson(log))
}

func TestSelectByIds(t *testing.T) {
	var logs []*StartupLog
	fmt.Println(mapper.SelectByIds([]string{"6733121fe9a67c280557c6d1", "67331275587bf0fa60630627"}, &logs))
	fmt.Println(json.ToJson(logs))
}

func TestSelectOne(t *testing.T) {
	data := StartupLog{Pid: 28}
	err := mapper.SelectOneByCond(&data, &data)
	fmt.Println(err)
	fmt.Println(json.ToJson(data))

	err = mapper.SelectOneByCond(&data, &data, "hostname")
	fmt.Println(err)
	fmt.Println(json.ToJson(data))

	err = mapper.SelectOneByBson(bson.M{"pid": 28}, &data)
	fmt.Println(err)
	fmt.Println(json.ToJson(data))
}

func TestSelect(t *testing.T) {
	data := StartupLog{Hostname: "998a29f641e6"}
	var dataList []*StartupLog
	err := mapper.SelectByCond(&data, mongostarter.NewOrderBy("_id", true), &dataList)
	fmt.Println(err)
	fmt.Println(json.ToJson(dataList))

	err = mapper.SelectByBson(bson.M{"hostname": "998a29f641e6"}, nil, &dataList)
	fmt.Println(err)
	fmt.Println(json.ToJson(dataList))
}

func TestCount(t *testing.T) {
	data := StartupLog{Hostname: "998a29f641e6"}
	fmt.Println(mapper.CountByCond(&data))
}

func TestSave(t *testing.T) {
	data := StartupLog{Hostname: "998a29f641e6"}
	fmt.Println(mapper.Insert(&data))
	data.ID = "123456"
	data.StartTime = mongostarter.Timestamp{Time: time.Now()}
	fmt.Println(mapper.Insert(&data))
}

func TestSaveBatch(t *testing.T) {
	data := []*StartupLog{
		{Hostname: "998a29f641e61111"},
		{Hostname: "998a29f641e11111"},
	}
	fmt.Println(mapper.InsertBatch(&data))

	many := []*bson.M{
		{"hostname": "123456"},
		{"hostname": "654321"},
	}
	fmt.Println(mapper.InsertBatchByBson(&many))

}

func TestPage(t *testing.T) {
	var dataList []*StartupLog
	total, err := mapper.SelectPageByCond(&StartupLog{}, nil, 1, 2, &dataList)
	fmt.Println(total, err)
	fmt.Println(json.ToJsonFormat(dataList))
	total, err = mapper.SelectPageByCollection(bson.M{"hostname": "998a29f641e6"}, nil, 2, 2, &dataList)
	fmt.Println(total, err)
	fmt.Println(json.ToJsonFormat(dataList))
}

func TestUpdateById(t *testing.T) {
	fmt.Println(mapper.UpdateById(&StartupLog{Pid: 29}, "67341bbe028d800a2c3c6075"))
	fmt.Println(mapper.UpdateByIdUseBson(bson.M{"hostname": "998a29f641e6", "pid": 28}, "67341bbe028d800a2c3c6074"))
}

func TestUpdateOne(t *testing.T) {
	cond := StartupLog{Hostname: "998a29f641e6"}
	upd := StartupLog{Pid: 1}
	fmt.Println(mapper.UpdateOneByCond(&cond, &upd))
	fmt.Println(mapper.UpdateOneByCondUseBson(bson.M{"hostname": "123"}, bson.M{"hostname": "998a29f641e1"}))
}

func TestUpdateMany(t *testing.T) {
	cond := StartupLog{Hostname: "998a29f641e11111"}
	upd := StartupLog{Pid: 1}
	fmt.Println(mapper.UpdateByCond(&upd, &cond))
}

func TestDelete(t *testing.T) {
	fmt.Println(mapper.DeleteById("67341b9ef567a4ecb209be4f"))
	fmt.Println(mapper.DeleteOneByCond(&StartupLog{Hostname: "998a29f641e6"}))
	fmt.Println(mapper.DeleteByCondUseBson(bson.M{"hostname": "998a29f641e6"}))
}
