package qdelayed

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"

	log "github.com/sirupsen/logrus"
)

type SimpleData struct {
	ID      string
	Message string
}

func (s SimpleData) String() string {
	return fmt.Sprintf("id:%s,message:%s", s.ID, s.Message)
}

type DelayedTestSuite struct {
	suite.Suite
	redisDB *redis.Client
	key     string
}

func (s *DelayedTestSuite) SetupSuite() {
	viper.SetDefault("redis.url", "192.168.1.233:30790")
	viper.SetDefault("redis.password", "12345678")

	s.redisDB = redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.url"),
		Password: viper.GetString("redis.password"),
		DB:       0,
	})
	s.key = "testdelayed"
}

func (s *DelayedTestSuite) TearDownSuite() {
	//s.redisDB.Del(s.key)
	s.redisDB.Close()
}

func (s *DelayedTestSuite) TestSimple() {
	testData := "aaa"
	testData2 := "bbb"
	delayed := NewRedisDelayed(s.redisDB, s.key)
	delayed.Add(time.Second, testData, testData2)

	done := make(chan struct{})
	go func() {
		defer close(done)
		rlt, err := delayed.Read(0, 10)
		if !s.NoError(err) {
			return
		}

		for i, r := range rlt {
			log.Infof("%d, %s", i, r.Data)
		}

	}()

	select {
	case <-time.After(2 * time.Second):
		s.FailNow("should not timeout")
	case <-done:

	}
}

func (s *DelayedTestSuite) TestAdd() {
	testData := &SimpleData{
		ID:      "1234",
		Message: "Hello world",
	}

	testData2 := &SimpleData{
		ID:      "5678",
		Message: "Goodby moon",
	}
	delayed := NewRedisDelayed(s.redisDB, s.key)
	delayed.Add(3*time.Second, testData, testData2)
}

func (s *DelayedTestSuite) TestRead() {
	delayed := NewRedisDelayed(s.redisDB, s.key)
	n, err := delayed.Read(time.Second, 2)

	if err != redis.Nil && !s.NoError(err) {
		return
	}

	log.Infof("%#v, %s", n, err)

}

func (s *DelayedTestSuite) TestReadUnmarshal() {
	delayed := NewRedisDelayed(s.redisDB, s.key, WithUnmarshalType(reflect.TypeOf(SimpleData{})))
	n, err := delayed.Read(time.Second, 2)

	if err != redis.Nil && !s.NoError(err) {
		return
	}

	if err == redis.Nil {
		return
	}

	for i, r := range n {
		log.Infof("%d, %d, %#v", i, r.TsNano, r.Data)
	}
}

func (s *DelayedTestSuite) TestRead2() {
	delayed := NewRedisDelayed(s.redisDB, s.key, WithUnmarshalType(reflect.TypeOf(SimpleData{})))

	done := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		for i := 0; i < 10; i++ {
			testData := &SimpleData{
				ID:      strconv.Itoa(i),
				Message: "Hello world",
			}
			if i < 5 {
				delayed.Add(time.Duration(5-i)*time.Second, testData)
			} else {
				delayed.Add(time.Second, testData)
			}
			time.Sleep(time.Second)
		}
		close(done)
	}()

	go func() {
		for {
			select {
			case <-done:
				log.Info("all done")
				close(done2)
				return
			default:
				log.Info("start read")
				n, err := delayed.Read(time.Second*10, 2)

				if err != nil && err != redis.Nil {
					log.Infof("read error: %s", err)
					return
				}

				if err != redis.Nil {
					for i, r := range n {
						log.Infof("get entry:%d, %d, %#v", i, (int64(time.Now().UnixNano())-r.TsNano)/int64(time.Millisecond), r.Data)
					}
				}
			}
		}
	}()

	<-done2
}

func (s *DelayedTestSuite) TestRead3() {
	delayed := NewRedisDelayed(s.redisDB, s.key, WithUnmarshalType(reflect.TypeOf(SimpleData{})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for i := 0; i < 10; i++ {
			testData := &SimpleData{
				ID:      strconv.Itoa(i),
				Message: "Hello world",
			}
			if i < 5 {
				delayed.Add(time.Duration(5-i)*time.Second, testData)
			} else {
				delayed.Add(time.Second, testData)
			}
			time.Sleep(time.Second)
		}
		time.Sleep(time.Second)
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("all done")
			return
		default:
			log.Info("start read")
			n, err := delayed.ReadWithContext(ctx, 10)

			if err != nil && err != redis.Nil {
				log.Infof("read error: %s", err)
				return
			}

			if err != redis.Nil {
				for i, r := range n {
					log.Infof("get entry:%d, %d, %#v", i, (int64(time.Now().UnixNano())-r.TsNano)/int64(time.Millisecond), r.Data)
				}
			}
		}
	}
}

func TestDelayed(t *testing.T) {
	suite.Run(t, new(DelayedTestSuite))
}

func BenchmarkSteamRead(b *testing.B) {
	viper.SetDefault("redis.url", "192.168.1.233:30790")
	viper.SetDefault("redis.password", "12345678")

	redisDB := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.url"),
		Password: viper.GetString("redis.password"),
		DB:       0,
	})

	defer redisDB.Close()
}
