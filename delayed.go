package qdelayed

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/kkkbird/quuid"
)

const (
	defaultPollInterval = 10 * time.Millisecond
)

// qdelayed predefined error
var (
	ErrNoData           = errors.New("No data")
	ErrDataTypeMismatch = errors.New("Data type mismatch")
)

// DelayedResult is struct for qdelayed result
type DelayedResult struct {
	TsNano int64
	Data   interface{}
}

// QDelayed qdelayed interface
type QDelayed interface {
	ReadWithContext(ctx context.Context, count int) ([]DelayedResult, error)
	Read(block time.Duration, count int) ([]DelayedResult, error)
	Add(delay time.Duration, data ...interface{}) error
	AddByDeadline(d time.Time, data ...interface{}) error
}

// QRedisDelayed qdelay redis implementation
type QRedisDelayed struct {
	db            *redis.Client
	key           string
	pollInterval  time.Duration
	uuidGen       *quuid.QUUID
	unmarshalType reflect.Type
}

func (r *QRedisDelayed) uuid() string {
	return r.uuidGen.UUID()
}

// AddByDeadline add a delayed entry by deadline
func (r *QRedisDelayed) AddByDeadline(d time.Time, data ...interface{}) error {
	if len(data) == 0 {
		return ErrNoData
	}
	score := float64(d.UnixNano())
	members := make([]*redis.Z, len(data))

	for i, d := range data {
		// if r.unmarshalType != nil && reflect.TypeOf(data[i]) != r.unmarshalType {
		// 	return ErrDataTypeMismatch
		// }

		id := r.uuid()

		var member string

		switch dt := d.(type) {
		case string:
			member = dt
		default:
			jstr, _ := json.Marshal(d)
			member = string(jstr)
		}
		// add uuid to make all member member
		members[i] = &redis.Z{Score: score, Member: id + ":" + string(member)}
	}
	r.db.ZAdd(r.key, members...)

	return nil
}

// Add add a delayed entry by delay duration
func (r *QRedisDelayed) Add(delay time.Duration, data ...interface{}) error {
	return r.AddByDeadline(time.Now().Add(delay), data...)
}

var zpopByScore = redis.NewScript(`
local message = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, ARGV[2]);
if #message > 0 then
  redis.call('ZREM', KEYS[1], unpack(message));
  return message;
else
  return nil;
end
`)

func (r *QRedisDelayed) ReadWithContext(ctx context.Context, count int) ([]DelayedResult, error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	for {
		nowTs := time.Now().UnixNano()
		m, err := zpopByScore.Run(r.db, []string{r.key}, nowTs, count).Result() // read will return redis.Nil if there is

		if err == nil {
			messages := m.([]interface{})
			length := len(messages) >> 1
			rlt := make([]DelayedResult, length)

			for i := 0; i < length; i++ {
				// remove uuid
				sData := strings.SplitN(messages[i*2].(string), ":", 2)[1]

				// check unmarshal function
				if r.unmarshalType == nil {
					rlt[i].Data = sData
				} else {
					data := reflect.New(r.unmarshalType)
					err = json.Unmarshal([]byte(sData), data.Interface())
					if err != nil {
						return nil, err
					}
					rlt[i].Data = data
				}

				tsNano, _ := strconv.ParseFloat(messages[i*2+1].(string), 64)
				rlt[i].TsNano = int64(tsNano)
			}

			return rlt, err
		}
		if err != redis.Nil {
			return nil, err
		}
		// err == redis.Nil

		select {
		case <-time.After(r.pollInterval):
		case <-ctx.Done():
			//return nil, ctx.Err()
			return nil, redis.Nil
		}
	}
}

func (r *QRedisDelayed) Read(block time.Duration, count int) ([]DelayedResult, error) {
	ctx := context.Background()
	var cancel context.CancelFunc

	if block > 0 {
		ctx, cancel = context.WithTimeout(ctx, block)
		defer cancel()
	}

	return r.ReadWithContext(ctx, count)
}

// RedisOpts is setters for application options
type RedisOpts func(a *QRedisDelayed)

// WithPollInterval set the poll interval
func WithPollInterval(d time.Duration) RedisOpts {
	return func(a *QRedisDelayed) {
		a.pollInterval = d
	}
}

// WithUnmarshalType set unmarshal type, qdelayed will return ptr of unmarshaled data instead of json string
func WithUnmarshalType(t reflect.Type) RedisOpts {
	return func(a *QRedisDelayed) {
		a.unmarshalType = t
	}
}

// NewRedisDelayed create a qdelayed queue
func NewRedisDelayed(db *redis.Client, key string, opts ...RedisOpts) QDelayed {
	delayed := &QRedisDelayed{
		db:           db,
		key:          key,
		pollInterval: defaultPollInterval,
		uuidGen:      quuid.New(quuid.WithHWAddressPrefix),
	}

	for _, opt := range opts {
		opt(delayed)
	}

	return delayed
}
