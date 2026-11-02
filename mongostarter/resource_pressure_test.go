package mongostarter

import (
	"context"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type memoryDocument struct {
	Payload string `bson:"payload"`
}

func memoryCursor(tb testing.TB, count int) *mongo.Cursor {
	tb.Helper()
	documents := make([]any, count)
	for i := range documents {
		documents[i] = bson.D{{Key: "payload", Value: strings.Repeat("x", 1024)}}
	}
	cursor, err := mongo.NewCursorFromDocuments(documents, nil, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return cursor
}

// TestResultSliceReuseDocumentsRetention records driver ownership semantics, not an unbounded cache leak.
func TestResultSliceReuseDocumentsRetention(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		rows := []*memoryDocument{{Payload: "old0"}, {Payload: "old1"}, {Payload: "old2"}}
		tail := rows[2]
		cursor := memoryCursor(t, 1)
		var err error
		if wrapped {
			err = checkMultipleResult(cursor, nil, &rows)
		} else {
			err = cursor.All(context.Background(), &rows)
		}
		if err != nil || len(rows) != 1 {
			t.Fatal("result decoding failed")
		}
		if rows[:cap(rows)][2] != tail {
			t.Fatal("driver slice reuse behavior changed; reassess retention guidance")
		}
		clear(rows[len(rows):cap(rows)])
		if rows[:cap(rows)][2] != nil {
			t.Fatal("unused tail was not cleared")
		}
	}
}

func BenchmarkResultDecode(b *testing.B) {
	for _, mode := range []string{"driver", "helper"} {
		b.Run(mode, func(b *testing.B) {
			documents := make([]any, 16)
			for i := range documents {
				documents[i] = bson.D{{Key: "payload", Value: strings.Repeat("x", 1024)}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cursor, err := mongo.NewCursorFromDocuments(documents, nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				var rows []*memoryDocument
				if mode == "helper" {
					err = checkMultipleResult(cursor, nil, &rows)
				} else {
					err = cursor.All(context.Background(), &rows)
				}
				if err != nil || len(rows) != 16 {
					b.Fatal("decode failed")
				}
			}
		})
	}
}
