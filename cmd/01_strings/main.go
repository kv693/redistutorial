// Topic 1: Strings, the simplest Redis type (binary-safe, up to 512 MB).
// Examples: basic SET/GET, missing keys, a page-view counter, a cache with TTL.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"

	"redis_tutorial/internal/rdb"
)

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	basicSetGet(ctx, client)
	missingKey(ctx, client)
	pageViewCounter(ctx, client)
	cacheWithTTL(ctx, client)
	setIfNotExists(ctx, client)
}

// SET / GET: store and read a plain value.
func basicSetGet(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== SET / GET ==")

	if err := c.Set(ctx, "tutorial:greeting", "hello redis", 0).Err(); err != nil { // 0 = no expiry
		log.Fatal(err)
	}
	val, err := c.Get(ctx, "tutorial:greeting").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("tutorial:greeting =", val)
}

// GET on a missing key returns redis.Nil. It's not a failure, so check for it.
func missingKey(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Missing key ==")

	_, err := c.Get(ctx, "tutorial:does-not-exist").Result()
	switch {
	case errors.Is(err, redis.Nil):
		fmt.Println("key does not exist (redis.Nil)")
	case err != nil:
		log.Fatal(err) // a real error, such as a network failure
	}
}

// INCR: an atomic counter. Redis handles concurrent callers, so no lost
// updates and no read-modify-write race in your code. The value is stored as a
// string but treated as an integer.
func pageViewCounter(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Page-view counter (INCR) ==")

	key := "tutorial:views:/home"
	c.Del(ctx, key) // start clean so the demo is repeatable

	for i := 0; i < 3; i++ {
		n, err := c.Incr(ctx, key).Result() // creates the key at 0 if missing
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("views after hit:", n)
	}

	n, _ := c.IncrBy(ctx, key, 10).Result() // add an arbitrary amount
	fmt.Println("after IncrBy 10:", n)

	// Read it back as an int. go-redis converts the string reply for you.
	total, _ := c.Get(ctx, key).Int()
	fmt.Println("GET as int:", total)
}

// SET with a TTL: the standard cache pattern. Redis deletes the key itself
// once the time is up.
func cacheWithTTL(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Cache with TTL ==")

	key := "tutorial:cache:user:42"
	c.Set(ctx, key, `{"id":42,"name":"Asha"}`, 2*time.Second)

	ttl, _ := c.TTL(ctx, key).Result()
	fmt.Println("TTL right after set:", ttl.Round(time.Second))

	val, _ := c.Get(ctx, key).Result()
	fmt.Println("cache hit:", val)

	time.Sleep(3 * time.Second)

	_, err := c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		fmt.Println("after 3s: expired, cache miss, so reload from the database")
	}
}

// SET NX ("only if not exists"): a building block for locks and for
// "first writer wins" logic.
func setIfNotExists(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== SET NX (lock-style) ==")

	key := "tutorial:lock:report"
	c.Del(ctx, key)

	first, _ := c.SetNX(ctx, key, "worker-1", 10*time.Second).Result()
	second, _ := c.SetNX(ctx, key, "worker-2", 10*time.Second).Result()
	owner, _ := c.Get(ctx, key).Result()

	fmt.Printf("worker-1 got lock: %v, worker-2 got lock: %v, owner: %s\n", first, second, owner)
}
