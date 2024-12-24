package mongostarter

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// GetBsonDValue 从bson document 中获取指定的属性值
func GetBsonDValue(name string, d bson.D) interface{} {
	if len(d) == 0 {
		return nil
	}
	for _, e := range d {
		if e.Key == name {
			return e.Value
		}
	}
	return nil
}

// CheckSingleResult 检查单条查询结果
func CheckSingleResult(singleResult *mongo.SingleResult, result interface{}) error {
	if singleResult.Err() != nil {
		return singleResult.Err()
	}
	return singleResult.Decode(result)
}

// CheckMultipleResult 检查多条查询结果
func CheckMultipleResult(cursor *mongo.Cursor, err error, result interface{}) error {
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	return cursor.All(context.Background(), result)
}

// CheckSingleSaveResult 检查单条插入结果
func CheckSingleSaveResult(result *mongo.InsertOneResult, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if !result.Acknowledged {
		return "", errors.New("not acknowledged")
	}
	objectId, ok := result.InsertedID.(bson.ObjectID)
	if ok {
		return objectId.Hex(), nil
	}
	return fmt.Sprintf("%v", result.InsertedID), nil
}

// CheckMultipleSaveResult 检查多条插入结果
func CheckMultipleSaveResult(result *mongo.InsertManyResult, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	if !result.Acknowledged {
		return nil, errors.New("not acknowledged")
	}
	objectIds := result.InsertedIDs
	var ids []string
	for _, v := range objectIds {
		objectId, ok := v.(bson.ObjectID)
		if ok {
			ids = append(ids, objectId.Hex())
		} else {
			ids = append(ids, fmt.Sprintf("%v", v))
		}
	}
	return ids, nil
}

// CheckUpdateResult 检查更新结果
func CheckUpdateResult(result *mongo.UpdateResult, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if !result.Acknowledged {
		return false, errors.New("not acknowledged")
	}
	return result.ModifiedCount > 0, nil
}

// CheckDeleteResult 检查删除结果
func CheckDeleteResult(result *mongo.DeleteResult, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if !result.Acknowledged {
		return false, errors.New("not acknowledged")
	}
	return result.DeletedCount > 0, nil
}
