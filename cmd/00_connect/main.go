// Topic 0: connect to Redis and run PING / INFO.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"redis_tutorial/internal/rdb"
)

func main() {
	ctx := context.Background()

	client, err := rdb.New(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// PING -> PONG
	pong, _ := client.Ping(ctx).Result()
	fmt.Println("PING ->", pong)

	// INFO server returns a text blob of "key:value" lines.
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "redis_version:") || strings.HasPrefix(line, "tcp_port:") {
			fmt.Println(line)
		}
	}
}
