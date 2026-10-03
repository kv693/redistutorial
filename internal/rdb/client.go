// Package rdb holds the connection helper shared by every demo.
package rdb

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

// New returns a client for REDIS_ADDR (default localhost:6379) and verifies
// the server is reachable with PING.
func New(ctx context.Context) (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 3 * time.Second,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("cannot reach redis at %s: %w", addr, err)
	}
	return client, nil
}
