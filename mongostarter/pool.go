package mongostarter

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// PoolConfig configures per-server pools before client creation.
// Non-nil fields override URI options; nil preserves URI or driver defaults.
type PoolConfig struct {
	// MaxPoolSize limits connections per server; zero means unlimited, and the driver default is 100.
	MaxPoolSize *uint64
	// MinPoolSize sets the maintained minimum; zero disables the minimum.
	// It must not exceed the effective nonzero MaxPoolSize.
	MinPoolSize *uint64
	// MaxConnecting limits concurrent connection establishment; zero uses the driver default of 2.
	MaxConnecting *uint64
	// MaxConnIdleTime limits connection idle time; zero disables the limit and negatives are rejected.
	MaxConnIdleTime *time.Duration
}

func clonePoolValue[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func clonePoolConfig(config PoolConfig) PoolConfig {
	return PoolConfig{
		MaxPoolSize:     clonePoolValue(config.MaxPoolSize),
		MinPoolSize:     clonePoolValue(config.MinPoolSize),
		MaxConnecting:   clonePoolValue(config.MaxConnecting),
		MaxConnIdleTime: clonePoolValue(config.MaxConnIdleTime),
	}
}

func applyPoolConfig(client *options.ClientOptions, config PoolConfig) error {
	if config.MaxConnIdleTime != nil && *config.MaxConnIdleTime < 0 {
		return fmt.Errorf("%w: MaxConnIdleTime must not be negative", ErrInvalidPoolConfig)
	}
	if config.MaxPoolSize != nil {
		client.SetMaxPoolSize(*config.MaxPoolSize)
	}
	if config.MinPoolSize != nil {
		client.SetMinPoolSize(*config.MinPoolSize)
	}
	if config.MaxConnecting != nil {
		client.SetMaxConnecting(*config.MaxConnecting)
	}
	if config.MaxConnIdleTime != nil {
		client.SetMaxConnIdleTime(*config.MaxConnIdleTime)
	}
	// Include the driver's default maximum when only a minimum was configured.
	maxPoolSize := uint64(100)
	if client.MaxPoolSize != nil {
		maxPoolSize = *client.MaxPoolSize
	}
	if client.MinPoolSize != nil && maxPoolSize != 0 && *client.MinPoolSize > maxPoolSize {
		return fmt.Errorf("%w: MinPoolSize=%d exceeds MaxPoolSize=%d", ErrInvalidPoolConfig, *client.MinPoolSize, maxPoolSize)
	}
	// ApplyURI retains parsing errors; explicit setters must not hide an invalid URI.
	return client.Validate()
}
