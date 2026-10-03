// Topic 3: Lists, ordered sequences of strings with fast push/pop at both ends.
// Examples: a job queue (producer/consumer, including a blocking consumer)
// and a capped "recent activity" feed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"redis_tutorial/internal/rdb"
)

// A job is serialized to JSON, because list elements are plain strings.
type Job struct {
	ID   int    `json:"id"`
	Task string `json:"task"`
}

const (
	queueKey  = "tutorial:queue:emails"
	recentKey = "tutorial:recent:activity"
)

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Del(ctx, queueKey, recentKey) // start clean so the demo is repeatable

	enqueueJobs(ctx, client)
	inspectQueue(ctx, client)
	consumeNonBlocking(ctx, client)
	blockingConsumer(ctx, client)
	recentActivityFeed(ctx, client)
}

// LPUSH adds to the head, RPOP removes from the tail, so the first job pushed
// is the first one popped (FIFO).
func enqueueJobs(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Producer: LPUSH ==")

	for i := 1; i <= 4; i++ {
		payload, _ := json.Marshal(Job{ID: i, Task: fmt.Sprintf("send welcome email #%d", i)})
		n, err := c.LPush(ctx, queueKey, payload).Result() // returns the new list length
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("queued job %d, queue length now %d\n", i, n)
	}
}

// LLEN / LRANGE / LINDEX: look at the queue without removing anything.
func inspectQueue(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Inspect: LLEN / LRANGE / LINDEX ==")

	n, _ := c.LLen(ctx, queueKey).Result()
	fmt.Println("length:", n)

	// Indexes are inclusive. 0 is the head, -1 is the tail.
	all, _ := c.LRange(ctx, queueKey, 0, -1).Result()
	fmt.Println("head -> tail:")
	for _, s := range all {
		fmt.Println("  ", s)
	}

	next, _ := c.LIndex(ctx, queueKey, -1).Result() // the tail is the next job to run
	fmt.Println("next to run:", next)
}

// RPOP: take one job. An empty list gives redis.Nil.
func consumeNonBlocking(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Consumer: RPOP (non-blocking) ==")

	for {
		raw, err := c.RPop(ctx, queueKey).Result()
		if errors.Is(err, redis.Nil) {
			fmt.Println("queue is empty")
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		var j Job
		_ = json.Unmarshal([]byte(raw), &j)
		fmt.Printf("processed job %d: %s\n", j.ID, j.Task)
	}
}

// BRPOP: wait for a job instead of polling in a loop. The worker sleeps on the
// server side until something is pushed or the timeout passes. Redis parks this
// client without blocking its other clients.
func blockingConsumer(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Consumer: BRPOP (blocking) ==")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("worker: waiting for a job...")
		start := time.Now()

		// Returns []string{key, value}. The key is included because BRPOP can
		// watch several lists at once.
		res, err := c.BRPop(ctx, 5*time.Second, queueKey).Result()
		if err != nil {
			fmt.Println("worker: gave up:", err) // redis.Nil on timeout
			return
		}
		fmt.Printf("worker: got %s after %v\n", res[1], time.Since(start).Round(100*time.Millisecond))
	}()

	time.Sleep(1 * time.Second) // the worker is already blocked by now
	fmt.Println("producer: pushing a job")
	payload, _ := json.Marshal(Job{ID: 99, Task: "urgent email"})
	c.LPush(ctx, queueKey, payload)

	wg.Wait()
}

// LPUSH + LTRIM: keep only the newest N entries, a bounded feed that cannot
// grow without limit.
func recentActivityFeed(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Capped feed: LPUSH + LTRIM ==")

	for i := 1; i <= 8; i++ {
		c.LPush(ctx, recentKey, fmt.Sprintf("event-%d", i))
		c.LTrim(ctx, recentKey, 0, 4) // keep indexes 0..4, which is 5 items
	}

	items, _ := c.LRange(ctx, recentKey, 0, -1).Result()
	fmt.Println("last 5 events, newest first:", items)
}
