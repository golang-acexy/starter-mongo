//go:build integration

package mongostarter

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

func poolIntegrationURI(t *testing.T) string {
	t.Helper()
	uri := os.Getenv("MONGO_TEST_URI")
	if uri == "" {
		t.Fatal("set MONGO_TEST_URI before running pool integration tests")
	}
	return uri
}

type poolObservation struct {
	mu                                                                                                         sync.Mutex
	created, ready, closed, idleClosed, checkedOut, peakCheckedOut, connecting, peakConnecting, checkoutFailed int
	max, min                                                                                                   uint64
}

func (o *poolObservation) observe(e *event.PoolEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch e.Type {
	case event.ConnectionPoolCreated:
		if e.PoolOptions != nil {
			o.max, o.min = e.PoolOptions.MaxPoolSize, e.PoolOptions.MinPoolSize
		}
	case event.ConnectionCreated:
		o.created++
		o.connecting++
		o.peakConnecting = max(o.peakConnecting, o.connecting)
	case event.ConnectionReady:
		o.ready++
		o.connecting--
	case event.ConnectionClosed:
		o.closed++
		if e.Reason == event.ReasonIdle {
			o.idleClosed++
		}
	case event.ConnectionCheckedOut:
		o.checkedOut++
		o.peakCheckedOut = max(o.peakCheckedOut, o.checkedOut)
	case event.ConnectionCheckedIn:
		o.checkedOut--
	case event.ConnectionCheckOutFailed:
		o.checkoutFailed++
	}
}

func connectPool(t *testing.T, pool PoolConfig, monitor *event.CommandMonitor) (*mongo.Client, *poolObservation) {
	t.Helper()
	observation := &poolObservation{}
	clientOptions := options.Client().ApplyURI(poolIntegrationURI(t))
	if err := applyPoolConfig(clientOptions, pool); err != nil {
		t.Fatal("invalid integration client options")
	}
	clientOptions.SetPoolMonitor(&event.PoolMonitor{Event: observation.observe})
	if monitor != nil {
		clientOptions.SetMonitor(monitor)
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		t.Fatal("could not construct integration client")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Disconnect(ctx); err != nil {
			t.Error("integration client disconnect failed")
		}
	})
	pingPool(t, client)
	return client, observation
}

func pingPool(t *testing.T, client *mongo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		t.Fatal("MongoDB Ping failed; check connection settings and authentication")
	}
}

func TestPoolIntegrationCapacity(t *testing.T) {
	var hold atomic.Bool
	gate := make(chan struct{})
	entered := make(chan struct{}, 2)
	monitor := &event.CommandMonitor{Started: func(ctx context.Context, e *event.CommandStartedEvent) {
		if e.CommandName != "ping" || !hold.Load() {
			return
		}
		// The driver has checked out a real connection before invoking this callback.
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}}
	client, observed := connectPool(t, PoolConfig{MaxPoolSize: new(uint64(2)), MinPoolSize: new(uint64(0)), MaxConnecting: new(uint64(1))}, monitor)
	hold.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() { defer workers.Done(); results <- client.Ping(ctx, readpref.Primary()) }()
	}
	var release sync.Once
	defer func() { release.Do(func() { close(gate) }); cancel(); workers.Wait() }()
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("connections were not checked out in time")
		}
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := client.Ping(waitCtx, readpref.Primary())
	waitCancel()
	if err == nil || !mongo.IsTimeout(err) {
		t.Fatal("expected timeout while the pool was exhausted")
	}
	observed.mu.Lock()
	valid := observed.max == 2 && observed.checkedOut == 2 && observed.peakCheckedOut == 2 && observed.checkoutFailed > 0 && observed.peakConnecting <= 1
	observed.mu.Unlock()
	if !valid {
		t.Fatal("pool events did not enforce capacity, wait, or connection establishment limits")
	}
	hold.Store(false)
	release.Do(func() { close(gate) })
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal("held Ping failed")
		}
	}
	pingPool(t, client)
}

func TestPoolIntegrationIdleExpiration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		idle    time.Duration
		expires bool
	}{
		{"expires", 100 * time.Millisecond, true},
		{"zero_disables", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, observed := connectPool(t, PoolConfig{MaxPoolSize: new(uint64(1)), MinPoolSize: new(uint64(0)), MaxConnIdleTime: &tc.idle}, nil)
			observed.mu.Lock()
			created, idleClosed := observed.created, observed.idleClosed
			observed.mu.Unlock()
			// Checkout prunes expired idle connections without waiting for the background maintenance interval.
			timer := time.NewTimer(200 * time.Millisecond)
			defer timer.Stop()
			<-timer.C
			pingPool(t, client)
			observed.mu.Lock()
			defer observed.mu.Unlock()
			if tc.expires {
				if observed.idleClosed <= idleClosed || observed.created <= created {
					t.Fatal("expired idle connection was not replaced")
				}
			} else if observed.idleClosed != idleClosed || observed.created != created {
				t.Fatal("zero idle duration did not preserve the connection")
			}
		})
	}
}

func TestPoolIntegrationMinimum(t *testing.T) {
	_, observed := connectPool(t, PoolConfig{MaxPoolSize: new(uint64(3)), MinPoolSize: new(uint64(2)), MaxConnecting: new(uint64(1))}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		observed.mu.Lock()
		ready := observed.min == 2 && observed.ready >= 2 && observed.created-observed.closed >= 2
		connecting := observed.peakConnecting
		observed.mu.Unlock()
		if connecting > 1 {
			t.Fatal("concurrent connection establishment exceeded MaxConnecting")
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("minimum pool size was not established")
		case <-ticker.C:
		}
	}
}

func TestPoolIntegrationStarter(t *testing.T) {
	starter := &MongoStarter{Config: MongoConfig{
		MongoURI: poolIntegrationURI(t), Database: "admin",
		Pool: PoolConfig{MaxPoolSize: new(uint64(2)), MinPoolSize: new(uint64(0)), MaxConnecting: new(uint64(1)), MaxConnIdleTime: new(time.Minute)},
	}}
	if _, err := starter.Start(); err != nil {
		t.Fatal("starter failed to connect; check integration connection settings")
	}
	t.Cleanup(func() {
		graceful, stopped, err := starter.Stop(5 * time.Second)
		if err != nil || !graceful || !stopped {
			t.Error("starter did not stop gracefully")
		}
	})
	client := RawMongoClient()
	if client == nil {
		t.Fatal("starter did not publish its client")
	}
	pingPool(t, client)
}
