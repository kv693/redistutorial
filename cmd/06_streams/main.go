// Topic 6: Streams, an append-only log of entries, each with an auto-generated
// ID and a set of field/value pairs. Unlike a list, entries are not removed
// when read, and consumer groups track who has processed what.
// Example: an order-events log processed by a group of workers.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"redis_tutorial/internal/rdb"
)

const (
	stream = "tutorial:stream:orders"
	group  = "billing"
)

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Del(ctx, stream) // start clean; this also drops the stream's groups

	appendEvents(ctx, client)
	readHistory(ctx, client)
	tailLive(ctx, client)
	consumerGroup(ctx, client)
}

// XADD: append an entry. "*" asks Redis to generate the ID, which has the form
// <millisecond-timestamp>-<sequence> and always increases.
func appendEvents(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== XADD ==")

	for i := 1; i <= 5; i++ {
		id, err := c.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			MaxLen: 1000, // cap the log so it cannot grow forever
			Approx: true, // "~": trim in whole internal blocks, which is much cheaper
			Values: map[string]any{"order": 1000 + i, "status": "created", "amount": 100 * i},
		}).Result()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("appended entry", id)
	}
}

// XLEN / XRANGE / XREVRANGE: read entries by ID range. Reading does not
// consume them, so you can re-read the log any time.
func readHistory(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== XLEN / XRANGE / XREVRANGE ==")

	n, _ := c.XLen(ctx, stream).Result()
	fmt.Println("entries:", n)

	// "-" and "+" mean the smallest and largest possible IDs.
	first2, _ := c.XRangeN(ctx, stream, "-", "+", 2).Result()
	fmt.Println("first two:")
	for _, m := range first2 {
		fmt.Println("  ", m.ID, m.Values)
	}

	last, _ := c.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	fmt.Println("newest:", last[0].ID, last[0].Values)
}

// XREAD with BLOCK: wait for new entries, like BRPOP but non-destructive.
// "$" means "only entries added after this call starts".
func tailLive(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== XREAD BLOCK (tail the log) ==")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("tailer: waiting for a new entry...")
		res, err := c.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, "$"},
			Block:   3 * time.Second,
		}).Result()
		if errors.Is(err, redis.Nil) {
			fmt.Println("tailer: timed out")
			return
		}
		if err != nil {
			log.Fatal(err)
		}
		m := res[0].Messages[0]
		fmt.Println("tailer: got", m.ID, m.Values)
	}()

	time.Sleep(500 * time.Millisecond)
	c.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"order": 1006, "status": "created", "amount": 600},
	})
	wg.Wait()
}

// Consumer groups: several workers share one stream. Redis delivers each entry
// to ONE worker in the group, and remembers it as "pending" until that worker
// acknowledges it. This fixes the lost-job problem of the plain list queue.
func consumerGroup(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Consumer group: XGROUP / XREADGROUP / XACK ==")

	// "0" = the group starts from the beginning of the stream. "$" would mean
	// only entries added from now on.
	if err := c.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		log.Fatal(err)
	}

	// worker-1 reads 3 entries. ">" means "entries never delivered to this group".
	w1 := readGroup(ctx, c, "worker-1", 3)
	for _, m := range w1 {
		fmt.Println("worker-1 processing", m.ID, m.Values["order"])
		c.XAck(ctx, stream, group, m.ID) // done: remove from the pending list
	}
	fmt.Println("worker-1 acked", len(w1), "entries")

	// worker-2 gets the next 3, then "crashes" without acking.
	w2 := readGroup(ctx, c, "worker-2", 3)
	for _, m := range w2 {
		fmt.Println("worker-2 received", m.ID, m.Values["order"], "...and crashes")
	}

	// XPENDING: entries delivered but never acknowledged.
	p, _ := c.XPending(ctx, stream, group).Result()
	fmt.Println("pending:", p.Count, "| by consumer:", p.Consumers)

	// XAUTOCLAIM: another worker takes over entries that have sat idle too long.
	time.Sleep(1 * time.Second)
	claimed, _, err := c.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: "worker-1",
		MinIdle:  500 * time.Millisecond, // only entries idle at least this long
		Start:    "0",
		Count:    10,
	}).Result()
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range claimed {
		fmt.Println("worker-1 took over", m.ID, m.Values["order"])
		c.XAck(ctx, stream, group, m.ID)
	}

	p, _ = c.XPending(ctx, stream, group).Result()
	fmt.Println("pending after recovery:", p.Count)
}

func readGroup(ctx context.Context, c *redis.Client, consumer string, n int64) []redis.XMessage {
	res, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    n,
		Block:    time.Second,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil // nothing new
	}
	if err != nil {
		log.Fatal(err)
	}
	return res[0].Messages
}
