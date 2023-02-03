package main

import (
	"context"

	"github.com/kkkbird/qdelayed"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

func main() {
	redisdb := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.231:31329",
		Password: "kuyiNo1",
		DB:       0,
	})
	defer redisdb.Close()

	delayed := qdelayed.NewRedisDelayed(redisdb, "mydelayed")

	for {
		rlt, err := delayed.Read(context.Background(), 0, 10)

		if err != nil { // will not return redis.Nil if block 0
			log.WithError(err).Infof("qdelayed read error")
			return
		}

		for i, r := range rlt {
			log.Infof("%d, %s", i, r.Data)
		}
	}
}
