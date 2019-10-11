# qdelayed

a delay queue base on redis zset

refer to: https://redislabs.com/ebook/part-2-core-concepts/chapter-6-application-components-in-redis/6-4-task-queues/6-4-2-delayed-tasks/

# example 

``` golang
package main

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/kkkbird/qdelayed"
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

	delayed.Add(delayDuration, testData)

	log.Infof("Message delayed, you could see the message after %d second(s)", delayDuration/time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		rlt, err := delayed.Read(0, 10)

		if err != nil { // will not return redis.Nil if block 0
			log.WithError(err).Infof("qdelayed read error")
			return
		}

		for i, r := range rlt {
			log.Infof("%d, %s", i, r.Data)
		}
	}()

	select {
	case <-time.After(delayDuration + time.Second):
		log.Error("SHOULD NOT DISPLAYED")
	case <-done:
		log.Info("Done")
	}

}
```

``` shell
INFO[0000] Message delayed, you could see the message after 3 second(s)
INFO[0003] 0, Hello world
INFO[0003] Done
```

## tips

1. use `ReadWithContext` instead of `Read` to make full use of go context