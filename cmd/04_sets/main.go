// Topic 4: Sets, unordered collections of unique strings.
// Examples: unique visitors, mutual friends (set algebra), and a raffle.
package main

import (
	"context"
	"fmt"
	"log"
	"sort"

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

	// start clean so the demo is repeatable
	client.Del(ctx,
		"tutorial:visitors:mon", "tutorial:visitors:tue",
		"tutorial:friends:asha", "tutorial:friends:ravi",
		"tutorial:raffle")

	uniqueVisitors(ctx, client)
	membership(ctx, client)
	mutualFriends(ctx, client)
	raffle(ctx, client)
}

// SADD: adding a value that is already present does nothing. The reply says
// how many members were actually new, which gives free de-duplication.
func uniqueVisitors(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Unique visitors: SADD / SCARD ==")

	key := "tutorial:visitors:mon"
	hits := []string{"u1", "u2", "u1", "u3", "u2", "u1"} // 6 page hits, 3 people

	for _, user := range hits {
		added, err := c.SAdd(ctx, key, user).Result() // 1 = new, 0 = already there
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("hit from %s -> new visitor: %v\n", user, added == 1)
	}

	n, _ := c.SCard(ctx, key).Result()
	fmt.Println("hits:", len(hits), "| unique visitors (SCARD):", n)
}

// SISMEMBER / SMEMBERS / SREM: test, list and remove members.
func membership(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== SISMEMBER / SMISMEMBER / SMEMBERS / SREM ==")

	key := "tutorial:visitors:mon"

	seen, _ := c.SIsMember(ctx, key, "u2").Result()
	fmt.Println("u2 visited:", seen)

	// Several checks in one round trip.
	multi, _ := c.SMIsMember(ctx, key, "u1", "u9").Result()
	fmt.Println("u1, u9 visited:", multi)

	members, _ := c.SMembers(ctx, key).Result()
	sort.Strings(members) // sets are unordered, so sort for stable output
	fmt.Println("all visitors:", members)

	removed, _ := c.SRem(ctx, key, "u3").Result()
	fmt.Println("removed u3:", removed)
}

// SINTER / SUNION / SDIFF: set algebra computed on the server.
func mutualFriends(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Mutual friends: SINTER / SUNION / SDIFF ==")

	asha, ravi := "tutorial:friends:asha", "tutorial:friends:ravi"
	c.SAdd(ctx, asha, "meera", "dev", "kabir", "tara")
	c.SAdd(ctx, ravi, "dev", "tara", "john")

	mutual, _ := c.SInter(ctx, asha, ravi).Result()
	either, _ := c.SUnion(ctx, asha, ravi).Result()
	suggest, _ := c.SDiff(ctx, asha, ravi).Result() // Asha's friends that Ravi lacks

	sort.Strings(mutual)
	sort.Strings(either)
	sort.Strings(suggest)
	fmt.Println("mutual friends (SINTER):      ", mutual)
	fmt.Println("everyone either knows (SUNION):", either)
	fmt.Println("suggest to Ravi (SDIFF):      ", suggest)

	// Cheaper when you only need the size.
	count, _ := c.SInterCard(ctx, 0, asha, ravi).Result()
	fmt.Println("mutual count (SINTERCARD):", count)

	// Store a result as a new set for reuse.
	c.SInterStore(ctx, "tutorial:friends:asha+ravi", asha, ravi)
	c.Del(ctx, "tutorial:friends:asha+ravi")
}

// SRANDMEMBER picks without removing, SPOP picks and removes.
func raffle(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Raffle: SRANDMEMBER / SPOP ==")

	key := "tutorial:raffle"
	c.SAdd(ctx, key, "ticket-1", "ticket-2", "ticket-3", "ticket-4", "ticket-5")

	peek, _ := c.SRandMember(ctx, key).Result()
	fmt.Println("peek at a random ticket (kept in the set):", peek)

	winner, _ := c.SPop(ctx, key).Result() // a winner can only be drawn once
	left, _ := c.SCard(ctx, key).Result()
	fmt.Println("winner (removed from the set):", winner, "| tickets left:", left)
}
