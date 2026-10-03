// Topic 5: Sorted sets, unique members, each with a float score. Members are
// kept ordered by score (ties broken alphabetically).
// Example: a game leaderboard.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"

	"redis_tutorial/internal/rdb"
)

const board = "tutorial:leaderboard"

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	client.Del(ctx, board, "tutorial:leaderboard:week1", "tutorial:leaderboard:week2") // start clean

	addScores(ctx, client)
	topPlayers(ctx, client)
	playerRankAndScore(ctx, client)
	updateScores(ctx, client)
	scoreRanges(ctx, client)
	removeAndPop(ctx, client)
	combineBoards(ctx, client)
}

// ZADD: insert members with scores. Adding an existing member updates its score.
func addScores(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== ZADD ==")

	n, err := c.ZAdd(ctx, board,
		redis.Z{Score: 1200, Member: "asha"},
		redis.Z{Score: 950, Member: "ravi"},
		redis.Z{Score: 1800, Member: "meera"},
		redis.Z{Score: 400, Member: "dev"},
		redis.Z{Score: 1500, Member: "kabir"},
	).Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("players added:", n)
}

// ZREVRANGE (ZRange with Rev): highest score first. Ranks are 0-based and the
// range is inclusive, so 0..2 means the top three.
func topPlayers(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== Top 3: ZRANGE ... REV WITHSCORES ==")

	top, err := c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key: board, Start: 0, Stop: 2, Rev: true,
	}).Result()
	if err != nil {
		log.Fatal(err)
	}
	for i, z := range top {
		fmt.Printf("#%d %-6s %.0f\n", i+1, z.Member, z.Score)
	}
}

// ZREVRANK / ZSCORE: where does one player stand?
func playerRankAndScore(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== ZREVRANK / ZSCORE ==")

	rank, _ := c.ZRevRank(ctx, board, "asha").Result() // 0-based, 0 = best
	score, _ := c.ZScore(ctx, board, "asha").Result()
	fmt.Printf("asha is #%d with %.0f points\n", rank+1, score)

	// A member that is not in the set gives redis.Nil.
	_, err := c.ZRevRank(ctx, board, "ghost").Result()
	if errors.Is(err, redis.Nil) {
		fmt.Println("ghost: not on the board (redis.Nil)")
	}

	total, _ := c.ZCard(ctx, board).Result()
	fmt.Println("players on the board:", total)
}

// ZINCRBY: atomic score change. The player moves in the ranking automatically.
func updateScores(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== ZINCRBY ==")

	newScore, _ := c.ZIncrBy(ctx, board, 700, "ravi").Result() // ravi wins a match
	rank, _ := c.ZRevRank(ctx, board, "ravi").Result()
	fmt.Printf("ravi now has %.0f points and is #%d\n", newScore, rank+1)
}

// ZRANGEBYSCORE / ZCOUNT: query by score instead of by rank.
func scoreRanges(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== By score: ZRANGEBYSCORE / ZCOUNT ==")

	// Players with 1000 to 1600 points, including both ends.
	mid, _ := c.ZRangeByScore(ctx, board, &redis.ZRangeBy{Min: "1000", Max: "1600"}).Result()
	fmt.Println("between 1000 and 1600:", mid)

	// "(" makes a bound exclusive, "+inf" means no upper limit.
	elite, _ := c.ZCount(ctx, board, "(1500", "+inf").Result()
	fmt.Println("strictly above 1500:", elite)
}

// ZREM / ZPOPMIN / ZPOPMAX: remove by member, or take the lowest/highest.
func removeAndPop(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== ZREM / ZPOPMIN / ZPOPMAX ==")

	c.ZRem(ctx, board, "dev")

	best, _ := c.ZPopMax(ctx, board).Result() // removes and returns the leader
	if len(best) > 0 {
		fmt.Printf("popped leader: %s (%.0f)\n", best[0].Member, best[0].Score)
	}

	left, _ := c.ZRangeWithScores(ctx, board, 0, -1).Result()
	fmt.Println("remaining, lowest first:")
	for _, z := range left {
		fmt.Printf("  %-6s %.0f\n", z.Member, z.Score)
	}
}

// ZUNIONSTORE: combine boards, here summing two weekly boards into a total.
func combineBoards(ctx context.Context, c *redis.Client) {
	fmt.Println("\n== ZUNIONSTORE (weekly -> total) ==")

	w1, w2 := "tutorial:leaderboard:week1", "tutorial:leaderboard:week2"
	c.ZAdd(ctx, w1, redis.Z{Score: 100, Member: "asha"}, redis.Z{Score: 50, Member: "ravi"})
	c.ZAdd(ctx, w2, redis.Z{Score: 70, Member: "asha"}, redis.Z{Score: 90, Member: "kabir"})

	total := "tutorial:leaderboard:total"
	c.Del(ctx, total)
	c.ZUnionStore(ctx, total, &redis.ZStore{Keys: []string{w1, w2}, Aggregate: "SUM"})

	res, _ := c.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: total, Start: 0, Stop: -1, Rev: true}).Result()
	for _, z := range res {
		fmt.Printf("  %-6s %.0f\n", z.Member, z.Score)
	}
	c.Del(ctx, total)
}
