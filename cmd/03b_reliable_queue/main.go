// Topic 3b: a reliable queue built from lists.
//
// A plain RPOP deletes the job as it hands it over, so a worker that crashes
// mid-job loses it. Here a job is moved atomically from "pending" to
// "processing" (BLMOVE), and only removed from "processing" once the work is
// done (the ack). Anything left in "processing" belongs to a dead worker and
// can be put back on the queue.
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

const (
	pendingKey    = "tutorial:rq:pending"
	processingKey = "tutorial:rq:processing"
)

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Del(ctx, pendingKey, processingKey) // start clean so the demo is repeatable

	for i := 1; i <= 3; i++ {
		client.LPush(ctx, pendingKey, fmt.Sprintf("job-%d", i))
	}
	show(ctx, client, "after enqueue")

	// Worker A finishes one job, then crashes while holding the next.
	fmt.Println("\n== Worker A ==")
	job, _ := claim(ctx, client)
	fmt.Println("A claimed", job)
	ack(ctx, client, job)
	fmt.Println("A finished", job)

	job, _ = claim(ctx, client)
	fmt.Println("A claimed", job, "...and crashes before acking")
	show(ctx, client, "after A crashed")

	// A reaper puts abandoned jobs back at the consumer end of the queue.
	fmt.Println("\n== Recovery ==")
	n := requeueAbandoned(ctx, client)
	fmt.Println("requeued jobs:", n)
	show(ctx, client, "after requeue")

	// Worker B drains everything, including the job A dropped.
	fmt.Println("\n== Worker B ==")
	for {
		job, err := claim(ctx, client)
		if errors.Is(err, redis.Nil) {
			fmt.Println("B: queue empty, shutting down")
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("B claimed", job)
		ack(ctx, client, job)
		fmt.Println("B finished", job)
	}
	show(ctx, client, "end")
}

// claim atomically moves the oldest pending job to the processing list and
// returns it. BLMOVE blocks up to the timeout when nothing is pending, and
// returns redis.Nil if the timeout passes.
// Because the move is one command, there is no moment when the job is in
// neither list.
func claim(ctx context.Context, c *redis.Client) (string, error) {
	return c.BLMove(ctx, pendingKey, processingKey, "RIGHT", "LEFT", 1*time.Second).Result()
}

// ack removes the finished job from the processing list.
// LREM key 1 value removes one occurrence of that value.
func ack(ctx context.Context, c *redis.Client, job string) {
	if err := c.LRem(ctx, processingKey, 1, job).Err(); err != nil {
		log.Fatal(err)
	}
}

// requeueAbandoned moves every job in the processing list back to the tail of
// the pending list, where it is picked up next. Each move is atomic, so a
// reaper crash cannot lose a job either.
//
// This demo treats everything in processing as abandoned. A real system would
// store a claim timestamp per job (for example in a sorted set) and requeue
// only jobs older than a visibility timeout, so live workers are not disturbed.
func requeueAbandoned(ctx context.Context, c *redis.Client) int {
	n := 0
	for {
		_, err := c.LMove(ctx, processingKey, pendingKey, "RIGHT", "RIGHT").Result()
		if errors.Is(err, redis.Nil) {
			return n
		}
		if err != nil {
			log.Fatal(err)
		}
		n++
	}
}

func show(ctx context.Context, c *redis.Client, label string) {
	pending, _ := c.LRange(ctx, pendingKey, 0, -1).Result()
	processing, _ := c.LRange(ctx, processingKey, 0, -1).Result()
	fmt.Printf("[%s] pending=%v processing=%v\n", label, pending, processing)
}
