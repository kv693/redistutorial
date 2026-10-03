// Topic 2: Hashes, a map of field -> value stored under one key.
// Example: user profiles, mapped to and from a Go struct.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"

	"redis_tutorial/internal/rdb"
)

// The `redis` tags give the hash field names. go-redis uses them for both
// HSET (struct -> fields) and Scan (fields -> struct).
type User struct {
	Name   string `redis:"name"`
	Email  string `redis:"email"`
	Age    int    `redis:"age"`
	Logins int    `redis:"logins"`
}

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	key := "tutorial:user:1001"
	client.Del(ctx, key) // start clean so the demo is repeatable

	saveProfile(ctx, client, key)
	readOneField(ctx, client, key)
	readSeveralFields(ctx, client, key)
	readWholeProfile(ctx, client, key)
	updateFields(ctx, client, key)
	deleteAndInspect(ctx, client, key)
}

// HSET: write the struct as separate fields of one hash.
func saveProfile(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== HSET (struct -> hash) ==")

	u := User{Name: "Asha", Email: "asha@example.com", Age: 29}
	n, err := c.HSet(ctx, key, u).Result() // returns how many fields were newly added
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("fields added:", n)
}

// HGET: read a single field without fetching the rest.
func readOneField(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== HGET ==")

	email, err := c.HGet(ctx, key, "email").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("email:", email)

	// A missing field gives redis.Nil, same as a missing string key.
	_, err = c.HGet(ctx, key, "phone").Result()
	if errors.Is(err, redis.Nil) {
		fmt.Println("phone: not set (redis.Nil)")
	}
}

// HMGET: read a chosen subset of fields in one round trip.
func readSeveralFields(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== HMGET ==")

	vals, err := c.HMGet(ctx, key, "name", "age", "phone").Result()
	if err != nil {
		log.Fatal(err)
	}
	// One slot per requested field. Missing fields come back as nil.
	for i, f := range []string{"name", "age", "phone"} {
		fmt.Printf("%s = %v\n", f, vals[i])
	}
}

// HGETALL: fetch every field and scan it into the struct.
func readWholeProfile(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== HGETALL (hash -> struct) ==")

	var u User
	if err := c.HGetAll(ctx, key).Scan(&u); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", u)

	// The raw form is a map[string]string. Every value is a string.
	raw, _ := c.HGetAll(ctx, key).Result()
	fmt.Println("raw map:", raw)
}

// HINCRBY / HSET on one field: update part of the object in place.
// With a JSON string you would read the whole value, change it and write it back.
func updateFields(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== Partial updates ==")

	for i := 0; i < 3; i++ {
		c.HIncrBy(ctx, key, "logins", 1) // atomic per-field counter
	}
	c.HSet(ctx, key, "email", "asha.new@example.com") // change one field

	logins, _ := c.HGet(ctx, key, "logins").Int()
	email, _ := c.HGet(ctx, key, "email").Result()
	fmt.Println("logins:", logins, "| email:", email)
}

// HEXISTS, HLEN, HKEYS, HDEL: inspect and shrink the hash.
func deleteAndInspect(ctx context.Context, c *redis.Client, key string) {
	fmt.Println("\n== HEXISTS / HLEN / HKEYS / HDEL ==")

	exists, _ := c.HExists(ctx, key, "age").Result()
	fmt.Println("has age:", exists)

	c.HDel(ctx, key, "age")

	n, _ := c.HLen(ctx, key).Result()
	keys, _ := c.HKeys(ctx, key).Result()
	fmt.Println("field count:", n, "| fields:", keys)
}
