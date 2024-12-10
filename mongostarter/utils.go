package mongostarter

import "go.mongodb.org/mongo-driver/v2/bson"

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
