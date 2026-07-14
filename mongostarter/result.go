package mongostarter

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// checkSingleResult 检查单条查询结果
func checkSingleResult(singleResult *mongo.SingleResult, result any) error {
	if singleResult.Err() != nil {
		return singleResult.Err()
	}
	return singleResult.Decode(result)
}

// checkMultipleResult 检查多条查询结果
func checkMultipleResult(cursor *mongo.Cursor, err error, result any) error {
	if err != nil {
		return err
	}
	defer cursor.Close(context.Background())
	return cursor.All(context.Background(), result)
}

// checkSingleInsertResult 检查单条插入结果
func checkSingleInsertResult(result *mongo.InsertOneResult, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if !result.Acknowledged {
		return "", ErrNotAcknowledged
	}
	objectID, ok := result.InsertedID.(bson.ObjectID)
	if ok {
		return objectID.Hex(), nil
	}
	return fmt.Sprintf("%v", result.InsertedID), nil
}

// checkMultipleInsertResult 检查多条插入结果
func checkMultipleInsertResult(result *mongo.InsertManyResult, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	if !result.Acknowledged {
		return nil, ErrNotAcknowledged
	}
	objectIDs := result.InsertedIDs
	var ids []string
	for _, v := range objectIDs {
		objectID, ok := v.(bson.ObjectID)
		if ok {
			ids = append(ids, objectID.Hex())
		} else {
			ids = append(ids, fmt.Sprintf("%v", v))
		}
	}
	return ids, nil
}

// checkUpdateResult 检查更新结果
func checkUpdateResult(result *mongo.UpdateResult, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if !result.Acknowledged {
		return 0, ErrNotAcknowledged
	}
	return result.ModifiedCount, nil
}

// checkDeleteResult 检查删除结果
func checkDeleteResult(result *mongo.DeleteResult, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if !result.Acknowledged {
		return 0, ErrNotAcknowledged
	}
	return result.DeletedCount, nil
}
