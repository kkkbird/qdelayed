package main

import (
	"context"
	"time"

	"strconv"

	"github.com/kkkbird/qdelayed"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const (
	delayDuration = 3 * time.Second
)

func main() {
	redisdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.231:31329",
		Password: "kuyiNo1",
		DB:       0,
	})
	defer redisdb.Close()

	testData := "Hello world"

	delayed := qdelayed.NewRedisDelayed(redisdb, "mydelayed")

	ctx := context.Background()

	for i := 0; ; i++ {
		for j := 0; j < 10; j++ {
			delayed.Add(ctx, delayDuration, testData+" "+strconv.Itoa(i)+" "+strconv.Itoa(j))
		}

		log.Infof("Message delayed, you could see the message after %d second(s)", delayDuration/time.Second)
		time.Sleep(time.Millisecond * 100)
	}
}
