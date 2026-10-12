package mongostarter

import (
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestPoolConfigPrecedence(t *testing.T) {
	const uri = "mongodb://localhost/app?maxPoolSize=80&minPoolSize=5&maxConnecting=4&maxIdleTimeMS=60000&connectTimeoutMS=3000"
	for _, tc := range []struct {
		name                 string
		pool                 PoolConfig
		max, min, connecting uint64
		idle                 time.Duration
	}{
		{"URI preserved", PoolConfig{}, 80, 5, 4, time.Minute},
		{"partial override", PoolConfig{MaxPoolSize: new(uint64(30))}, 30, 5, 4, time.Minute},
		{"all overridden", PoolConfig{MaxPoolSize: new(uint64(20)), MinPoolSize: new(uint64(2)), MaxConnecting: new(uint64(3)), MaxConnIdleTime: new(5 * time.Minute)}, 20, 2, 3, 5 * time.Minute},
		{"explicit zeros", PoolConfig{MaxPoolSize: new(uint64(0)), MinPoolSize: new(uint64(0)), MaxConnecting: new(uint64(0)), MaxConnIdleTime: new(time.Duration(0))}, 0, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := options.Client().ApplyURI(uri)
			if err := applyPoolConfig(client, tc.pool); err != nil {
				t.Fatal(err)
			}
			if *client.MaxPoolSize != tc.max || *client.MinPoolSize != tc.min || *client.MaxConnecting != tc.connecting || *client.MaxConnIdleTime != tc.idle {
				t.Fatal("pool options do not follow field-level precedence")
			}
			if *client.ConnectTimeout != 3*time.Second || client.GetURI() != uri {
				t.Fatal("unrelated URI settings changed")
			}
		})
	}
	client := options.Client().ApplyURI("mongodb://localhost/app")
	if err := applyPoolConfig(client, PoolConfig{}); err != nil {
		t.Fatal(err)
	}
	if client.MaxPoolSize != nil || client.MinPoolSize != nil || client.MaxConnecting != nil || client.MaxConnIdleTime != nil {
		t.Fatal("empty configuration must leave driver defaults unset")
	}
}

func TestPoolConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name, uri string
		pool      PoolConfig
		invalid   bool
	}{
		{"negative idle", "mongodb://localhost/app", PoolConfig{MaxConnIdleTime: new(-time.Second)}, true},
		{"explicit max versus URI min", "mongodb://localhost/app?minPoolSize=10", PoolConfig{MaxPoolSize: new(uint64(5))}, true},
		{"explicit min versus URI max", "mongodb://localhost/app?maxPoolSize=5", PoolConfig{MinPoolSize: new(uint64(10))}, true},
		{"default max", "mongodb://localhost/app", PoolConfig{MinPoolSize: new(uint64(101))}, true},
		{"URI min versus default max", "mongodb://localhost/app?minPoolSize=101", PoolConfig{}, true},
		{"equal limits", "mongodb://localhost/app?maxPoolSize=5", PoolConfig{MinPoolSize: new(uint64(5))}, false},
		{"unlimited max", "mongodb://localhost/app?minPoolSize=200", PoolConfig{MaxPoolSize: new(uint64(0))}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := applyPoolConfig(options.Client().ApplyURI(tc.uri), tc.pool)
			if tc.invalid && !errors.Is(err, ErrInvalidPoolConfig) {
				t.Fatalf("expected pool validation error, got %v", err)
			}
			if !tc.invalid && err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, uri := range []string{"invalid://localhost/app", "mongodb://localhost/app?maxPoolSize=invalid"} {
		if err := applyPoolConfig(options.Client().ApplyURI(uri), PoolConfig{MaxPoolSize: new(uint64(20))}); err == nil {
			t.Fatal("explicit pool settings hid an invalid URI")
		}
	}
}

func TestPoolConfigSnapshot(t *testing.T) {
	max, min, connecting, idle := uint64(50), uint64(5), uint64(3), time.Minute
	original := PoolConfig{MaxPoolSize: &max, MinPoolSize: &min, MaxConnecting: &connecting, MaxConnIdleTime: &idle}
	calls := 0
	starter := &MongoStarter{
		Config:     MongoConfig{Pool: PoolConfig{MaxPoolSize: new(uint64(1))}},
		LazyConfig: func() MongoConfig { calls++; return MongoConfig{Pool: original} },
	}
	config := starter.getConfig()
	max, min, connecting, idle = 0, 0, 0, 0
	if *config.Pool.MaxPoolSize != 50 || *config.Pool.MinPoolSize != 5 || *config.Pool.MaxConnecting != 3 || *config.Pool.MaxConnIdleTime != time.Minute {
		t.Fatal("pool snapshot changed after external mutation")
	}
	client := options.Client().ApplyURI("mongodb://localhost/app")
	if err := applyPoolConfig(client, config.Pool); err != nil {
		t.Fatal(err)
	}
	*client.MaxPoolSize = 99
	if *config.Pool.MaxPoolSize != 50 {
		t.Fatal("driver options alias cached configuration")
	}
	if starter.getConfig() != config || calls != 1 {
		t.Fatal("lazy configuration was not cached once")
	}
}

func TestStartRejectsInvalidPoolBeforeConnecting(t *testing.T) {
	starter := &MongoStarter{Config: MongoConfig{
		MongoURI: "mongodb://127.0.0.1:1/app?maxPoolSize=5",
		Pool:     PoolConfig{MinPoolSize: new(uint64(10))},
	}}
	for range 2 {
		instance, err := starter.Start()
		if instance != nil || !errors.Is(err, ErrInvalidPoolConfig) {
			t.Fatalf("expected pool error before connection, got %v", err)
		}
		mongoLock.Lock()
		stopped := mongoState == lifecycleStopped
		mongoLock.Unlock()
		if !stopped || RawMongoClient() != nil {
			t.Fatal("failed validation left active lifecycle state")
		}
	}
}
